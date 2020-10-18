package com.ververica.platform.io.source;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import com.ververica.platform.entities.Email;
import org.apache.james.mime4j.dom.Message;
import org.apache.james.mime4j.dom.MessageBuilder;
import org.apache.james.mime4j.dom.address.Mailbox;
import org.apache.james.mime4j.dom.address.MailboxList;
import org.apache.james.mime4j.mboxiterator.CharBufferWrapper;
import org.apache.james.mime4j.mboxiterator.MboxIterator;
import org.apache.james.mime4j.message.DefaultMessageBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.CharConversionException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.SignStyle;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.ververica.platform.io.source.GithubSource.dateToLocalDateTime;
import static java.time.temporal.ChronoField.MONTH_OF_YEAR;
import static java.time.temporal.ChronoField.YEAR;

public class ApacheMboxSource extends RichSourceFunction<Email> implements CheckpointedFunction {

  private static final Logger LOG = LoggerFactory.getLogger(ApacheMboxSource.class);

  private static final DateTimeFormatter MBOX_DATE_FORMATTER =
          new DateTimeFormatterBuilder()
                  .parseCaseInsensitive()
                  .appendValue(YEAR, 4, 4, SignStyle.EXCEEDS_PAD)
                  .appendValue(MONTH_OF_YEAR, 2)
                  .parseDefaulting(ChronoField.DAY_OF_MONTH, 1)
                  .toFormatter();

  public static final List<CharsetEncoder> ENCODERS = Arrays.asList(
          StandardCharsets.ISO_8859_1.newEncoder(),
          StandardCharsets.UTF_8.newEncoder(),
          StandardCharsets.US_ASCII.newEncoder(),
          StandardCharsets.UTF_16.newEncoder(),
          StandardCharsets.UTF_16BE.newEncoder(),
          StandardCharsets.UTF_16LE.newEncoder());

  private final String listName;

  private LocalDateTime lastDate;

  private volatile boolean running = true;

  private transient ListState<LocalDateTime> state;

  private transient File mboxFile = null;

  public ApacheMboxSource(String listName) {
    this(listName, LocalDateTime.now().withDayOfMonth(1));
  }

  public ApacheMboxSource(String listName, LocalDateTime startDate) {
    this.listName = listName;
    this.lastDate = startDate;
  }

  @Override
  public void run(SourceContext<Email> ctx) throws IOException, InterruptedException {
    LocalDateTime nextPollTime = LocalDateTime.MIN;
    while (running) {
      if (nextPollTime.isAfter(LocalDateTime.now())) {
        //noinspection BusyWait
        Thread.sleep(10_000);
        continue;
      }

      String url = "http://mail-archives.apache.org/mod_mbox/" + listName + "/" + MBOX_DATE_FORMATTER.format(lastDate) + ".mbox";
      LOG.debug("Fetching mails from {}", url);

      InputStream in = new URL(url).openStream();
      Files.copy(in, mboxFile.toPath(), StandardCopyOption.REPLACE_EXISTING);

      List<Email> emails = null;
      for (CharsetEncoder encoder : ENCODERS) {
        encoder.reset();
        try (MboxIterator mboxIterator = MboxIterator.fromFile(mboxFile).charset(encoder.charset())
                .build()) {
          LOG.debug("Decoding with {}", encoder);

          emails = StreamSupport.stream(mboxIterator.spliterator(), false)
                  .map(message -> fromMessage(message, encoder.charset()))
                  .filter(email -> email.getDate().isAfter(lastDate))
                  .collect(Collectors.toList());

          LOG.debug("Found {} messages", emails.size());
          break;
        } catch (CharConversionException | IllegalArgumentException ex) {
          // Not the right encoder
        } catch (IOException ex) {
          LOG.warn("Failed to open mbox file {} downloaded from {}", mboxFile.getName(), url, ex);
        }
      }

      if (emails == null) {
        throw new IOException("No valid charset found");
      }

      boolean isCurrentMonth = lastDate.withDayOfMonth(1).truncatedTo(ChronoUnit.DAYS)
              .isEqual(LocalDateTime.now().withDayOfMonth(1).truncatedTo(ChronoUnit.DAYS));
      long maxTimestamp = 0L;
      LocalDateTime maxDate = null;
      synchronized (ctx.getCheckpointLock()) {
        for (Email email : emails) {
          long timestamp =
                  email.getDate().atZone(GithubSource.EVALUATION_ZONE).toInstant().toEpochMilli();
          ctx.collectWithTimestamp(email, timestamp);
          if (timestamp > maxTimestamp) {
            maxTimestamp = timestamp;
            maxDate = email.getDate();
          }
        }

        // If current month is reached, only poll once per hour for new data.
        // Also update next date to allow according with the highest seen (current month) or
        // assumed (for previous months).
        final LocalDateTime nextDate;
        if (isCurrentMonth) {
          nextPollTime = LocalDateTime.now().plus(1, ChronoUnit.HOURS);
          if (maxDate == null) {
            nextDate = lastDate;
          } else {
            nextDate = maxDate;
          }
        } else {
          // assume month is complete
          nextDate = lastDate
              .withDayOfMonth(1)
              .truncatedTo(ChronoUnit.DAYS)
              .plus(1, ChronoUnit.MONTHS);
          nextPollTime = nextDate;
        }

        lastDate = nextDate;
        long nextDateMillis = nextDate.atZone(GithubSource.EVALUATION_ZONE)
                .toInstant()
                .toEpochMilli();
        ctx.emitWatermark(new Watermark(nextDateMillis - 1));
      }
    }
  }

  private static Email fromMessage(CharBufferWrapper message, Charset charset) {
    try (InputStream in = message.asInputStream(charset)) {
      MessageBuilder builder = new DefaultMessageBuilder();
      Message email = builder.parseMessage(in);

      LocalDateTime date = dateToLocalDateTime(email.getDate());

      String fromStr = getAuthor(email);
      return Email.builder()
              .date(date)
              .from(fromStr)
              .subject(email.getSubject())
              .build();
    } catch (Exception e) {
      throw new RuntimeException("Failed to parse email", e);
    }
  }

  private static String getAuthor(Message email) {
    MailboxList from = email.getFrom();
    if (from != null) {
      return from.get(0).toString();
    } else {
      Mailbox sender = email.getSender();
      if (sender != null) {
        return sender.toString();
      } else {
        return "unknown";
      }
    }
  }

  @Override
  public void cancel() {
    running = false;
  }

  @Override
  public void open(Configuration configuration) throws IOException {
    mboxFile = File.createTempFile("temp", null);
  }

  @Override
  public void close() throws Exception {
    if (mboxFile != null) {
      if (!mboxFile.delete()) {
        throw new IOException("Failed to delete file " + mboxFile);
      }
    }
  }

  @Override
  public void snapshotState(FunctionSnapshotContext ctx) throws Exception {
    state.clear();
    state.add(lastDate);
  }

  @Override
  public void initializeState(FunctionInitializationContext ctx) throws Exception {
    state =
        ctx.getOperatorStateStore()
            .getListState(new ListStateDescriptor<>("lastDate", LocalDateTime.class));

    if (ctx.isRestored()) {
      Iterator<LocalDateTime> data = state.get().iterator();
      if (data.hasNext()) {
        lastDate = data.next();
      }
    }
  }
}
