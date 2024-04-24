package kafka.service;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import jakarta.annotation.PostConstruct;
import kafka.config.LocalDataTimeDeserializer;
import kafka.model.Data;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.kafka.receiver.KafkaReceiver;

import java.time.LocalDateTime;

@Service
@RequiredArgsConstructor
public class KafkaDataReceiverImpl implements KafkaDataReceiver{

    private final KafkaReceiver<String, Object> receiver;
    private final LocalDataTimeDeserializer localDataTimeDeserializer;
    private final KafkaDataService kafkaDataService;

    //PostConstruct для запуска fecth сразу при запуске приложения
      @PostConstruct
      private void init(){
          fecth();
      }

    /**
     * Начинает чтение из очереди
     */
    @Override
    public void fecth() {
        // В Gson регистрируется адаптер(как обрбатывается LocalDataTime c помощью реализованного LocalDataTimeDeserializer)
        Gson gson = new GsonBuilder()
            .registerTypeAdapter(LocalDateTime.class, localDataTimeDeserializer)
            .create();
        //когда обрабатывывается событие receive и приходит ответ, subscride обрабатывает сообщение
        receiver.receive()
                .subscribe(r ->{
                    Data data = gson.fromJson(r.value().toString(), Data.class);
                    kafkaDataService.handle(data);
                    r.receiverOffset().acknowledge(); //говорю кафке что сообщение получено и обработано. Нужно присылать следующее сообщение
                });



    }
}
