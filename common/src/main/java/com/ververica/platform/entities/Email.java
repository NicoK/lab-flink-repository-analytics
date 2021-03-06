package com.ververica.platform.entities;

import java.lang.reflect.Type;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.typeinfo.TypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@TypeInfo(Email.CommitTypeInfoFactory.class)
public class Email {
  private LocalDateTime date;
  private String fromRaw;
  private String fromEmail;
  private String subject;
  private String textBody;
  private String htmlBody;

  public static class CommitTypeInfoFactory extends TypeInfoFactory<Email> {
    @Override
    public TypeInformation<Email> createTypeInfo(
        Type t, Map<String, TypeInformation<?>> genericParameters) {
      Map<String, TypeInformation<?>> fields =
          new HashMap<String, TypeInformation<?>>() {
            {
              put("date", Types.LOCAL_DATE_TIME);
              put("fromRaw", Types.STRING);
              put("fromEmail", Types.STRING);
              put("htmlBody", Types.STRING);
              put("subject", Types.STRING);
              put("textBody", Types.STRING);
            }
          };
      return Types.POJO(Email.class, fields);
    }
  }
}
