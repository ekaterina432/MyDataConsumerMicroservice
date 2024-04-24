package kafka.service;

import kafka.model.Data;
import org.springframework.stereotype.Service;

@Service
public class KafkaDataServiceImpl implements KafkaDataService{
    @Override
    public void handle(Data data) {
        System.out.println("Объект данных получен: " + data.toString());


    }
}
