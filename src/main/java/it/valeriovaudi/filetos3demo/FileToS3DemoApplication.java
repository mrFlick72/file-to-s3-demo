package it.valeriovaudi.filetos3demo;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.http.MediaType;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.integration.core.GenericHandler;
import org.springframework.integration.dsl.*;
import org.springframework.integration.file.dsl.Files;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.integration.transformer.Transformer;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.function.HandlerFunction;
import org.springframework.web.servlet.function.RouterFunction;
import org.springframework.web.servlet.function.ServerRequest;
import org.springframework.web.servlet.function.ServerResponse;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;

import static org.springframework.web.servlet.function.RouterFunctions.route;

@EnableIntegration
@SpringBootApplication
public class FileToS3DemoApplication {

    public static void main(String[] args) {
        SpringApplication.run(FileToS3DemoApplication.class, args);
    }

    @Bean
    public PublishSubscribeChannelSpec<?> publishSubscribeChannel() {
        return MessageChannels.publishSubscribe();
    }

    @Bean
    public IntegrationFlow loadFromFile(@Value("${file.inbound-path:sample-data}") String inboundPath) {
        return IntegrationFlow.from(Files.inboundAdapter(new File(inboundPath))
                                .patternFilter("*.txt"),
                        e -> e.poller(Pollers.fixedDelay(1000)))
                .transform((Transformer) message -> {
                    System.out.println(message);
                    File file = (File) message.getPayload();
                    FileWithStatistics fileWithStatistics = new FileWithStatistics(
                            null,
                            file.getName(),
                            LocalDateTime.now(),
                            file.length(),
                            getContent(file)
                    );
                    return MessageBuilder.withPayload(fileWithStatistics)
                            .copyHeaders(message.getHeaders())
                            .build();
                })
                .channel("publishSubscribeChannel")
                .get();
    }

    static byte[] getContent(File file) {
        try {
            return java.nio.file.Files.readAllBytes(file.toPath());
        } catch (IOException e) {
            return new byte[0];
        }
    }

    @Bean
    public IntegrationFlow loadToS3(S3FileRepository repository) {
        return IntegrationFlow.from("publishSubscribeChannel")
                .handle((GenericHandler<FileWithStatistics>) (payload, headers) -> {
                    System.out.println("loadToS3");
                    System.out.println(payload);
                    repository.load(payload);
                    return payload.id;
                })
                .nullChannel();
    }

    @Bean
    public IntegrationFlow storeStatisticsToDB(FileStatisticsRepository repository) {
        return IntegrationFlow.from("publishSubscribeChannel")
                .log()
                .handle((GenericHandler<FileWithStatistics>) (payload, headers) -> {
                    System.out.println("storeStatisticsToDB");
                    System.out.println(payload);
                    repository.save(payload);
                    return payload.id;
                })
                .nullChannel();
    }

    @Bean
    public S3Client s3Client(AwsCredentialsProvider awsCredentialsProvider) {
        return S3Client.builder()
                .credentialsProvider(awsCredentialsProvider)
                .region(Region.US_EAST_1)
                .endpointOverride(URI.create("http://127.0.0.1:4566"))
                .build();
    }

    @Bean
    public AwsCredentialsProvider awsCredentialsProvider() {
        return StaticCredentialsProvider.create(AwsBasicCredentials.create("xxx", "xxx"));
    }
}

@Entity
class FileWithStatistics {
    @Id
    @GeneratedValue
    public Long id;
    public String name;
    public LocalDateTime timestamp;
    public long size;
    public transient byte[] content;

    public FileWithStatistics() {
    }

    public FileWithStatistics(Long id, String name, LocalDateTime timestamp, long size, byte[] content) {
        this.id = id;
        this.name = name;
        this.timestamp = timestamp;
        this.size = size;
        this.content = content;
    }

    @Override
    public String toString() {
        return "FileWithStatistics{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", timestamp=" + timestamp +
                ", size=" + size +
                ", content=" + Arrays.toString(content) +
                '}';
    }
}

interface FileStatisticsRepository extends JpaRepository<FileWithStatistics, Long> {
}

@Configuration
class FileStatisticsAuditorEndPoint {

    @Bean
    public RouterFunction<ServerResponse> routes(FileStatisticsRepository repository) {
        return route()
                .GET("/file-statistics", request -> {
                    List<FileWithStatistics> fileWithStatistics = repository.findAll();
                    return ServerResponse.ok()
                            .contentType(MediaType.APPLICATION_JSON)
                            .body(fileWithStatistics);
                })
                .build();
    }

}

@Service
class S3FileRepository {
    private final S3Client client;

    S3FileRepository(S3Client client) {
        this.client = client;
    }

    public void load(FileWithStatistics file) {
        client.putObject(
                PutObjectRequest.builder()
                        .bucket("file-to-s3-demo")
                        .key(file.name)
                        .build(),
                RequestBody.fromBytes(file.content)
        );
    }
}
