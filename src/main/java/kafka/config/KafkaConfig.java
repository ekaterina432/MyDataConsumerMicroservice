package kafka.config;

import com.jcabi.xml.XML;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


@Configuration
@RequiredArgsConstructor
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String servers;

    @Value("${topics}")
    private List<String> topics;

    private final XML setting;

    @Bean
    public Map<String, Object> receiverProperties(){
        Map<String, Object> props = new HashMap<>(5);
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,
                new TextXpath(this.setting, "//groupId").toString());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                new TextXpath(this.setting, "//keyDeserializer").toString());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                new TextXpath(this.setting, "//valueDeserializer").toString());
        props.put("spring.json.trusted.package", //для работы с пакетами
                new TextXpath(this.setting, "//trustedPackages").toString());
        return props;
    }

    /**
     *  Получение сообщений из Kafka топиков
     */
    @Bean
    public ReceiverOptions<String, Object> receiverOptions(){
        ReceiverOptions<String, Object> receiverOptions = ReceiverOptions
                .create(receiverProperties());
        return receiverOptions.subscription(topics)
                .addAssignListener(partitions -> System.out.println("подключился: " + partitions))//для вывода о том, что произошла подписка на какую-то партицию, в которую положили сообщение
                .addRevokeListener(partitions -> System.out.println("отключился: "+ partitions));//для вывода о том, что произошло отключения партиции
    }

    /**
     * KafkaReceiver нужен для:
     *1)Приема сообщений из одного или нескольких топиков Kafka;
     *2)Получение данных происходит асинхронно, что позволяет продолжать выполнение других операций без блокировки;
     *3)Обработки большого объема данных;
     *4)Настраивает параметры получения, такие как количество потоков, параметры чтения и так далее, для оптимизации производительности и масштабируемости;
     *5)Обрабатывает  ошибки и переподключаться к брокеру Kafka в случае потери связи или других проблем связи;
     */
    @Bean
    public KafkaReceiver<String, Object> receiver( ReceiverOptions<String, Object> receiverOptions){
        return KafkaReceiver.create(receiverOptions);
    }


}
