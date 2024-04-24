package kafka.service;

import kafka.model.Data;


public interface KafkaDataService {
    /**
     * сервис обрабатывает полученные данные
     */
    void handle(Data data);
}
