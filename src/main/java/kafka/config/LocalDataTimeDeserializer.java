package kafka.config;

import com.google.gson.*;
import lombok.RequiredArgsConstructor;
import org.springframework.http.codec.json.Jackson2SmileDecoder;
import org.springframework.stereotype.Component;

import java.lang.reflect.Type;
import java.time.LocalDateTime;

/**
 * Десириализация массива в LocalDateTime
 */
@Component
public class LocalDataTimeDeserializer implements JsonDeserializer {

    @Override
    public LocalDateTime deserialize(JsonElement jsonElement, Type type, JsonDeserializationContext jsonDeserializationContext) throws JsonParseException {
        JsonArray jsonArray = jsonElement.getAsJsonArray();
        int year  = jsonArray.get(0).getAsInt();
        int month = jsonArray.get(1).getAsInt();
        int day = jsonArray.get(2).getAsInt();
        int hour = jsonArray.get(3).getAsInt();
        int minute = jsonArray.get(4).getAsInt();
        int second = jsonArray.get(5).getAsInt();
        return LocalDateTime.of(year, month, day, hour, minute, second);
    }
}
