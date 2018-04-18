package org.superbiz.moviefun;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.servlet.ServletRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.superbiz.moviefun.blobstore.BlobStore;
import org.superbiz.moviefun.blobstore.S3Store;
import org.superbiz.moviefun.blobstore.ServiceCredentials;

@SpringBootApplication
public class Application {

    public static void main(String... args) {
        SpringApplication.run(Application.class, args);
    }

    @Bean
    public ServletRegistrationBean actionServletRegistration(ActionServlet actionServlet) {
        return new ServletRegistrationBean(actionServlet, "/moviefun/*");
    }

    @Bean
    public BlobStore blobStore(
            ServiceCredentials serviceCredentials,
            @Value("${s3.endpointUrl:#{null}}") String s3EndpointUrl
    ) {
        String s3AccessKey = serviceCredentials.getCredential("moviefun-s3", "aws-s3", "access_key_id");
        String s3SecretKey = serviceCredentials.getCredential("moviefun-s3", "aws-s3", "secret_access_key");
        String s3BucketName = serviceCredentials.getCredential("moviefun-s3", "aws-s3", "bucket");

        AWSCredentials credentials = new BasicAWSCredentials(s3AccessKey, s3SecretKey);
        AmazonS3Client s3Client = new AmazonS3Client(credentials);

        if (s3EndpointUrl != null) {
            s3Client.setEndpoint(s3EndpointUrl);
        }

        return new S3Store(s3Client, s3BucketName);
    }

    @Bean
    ServiceCredentials serviceCredentials(@Value("${vcap.services}") String vcapServices) {
        return new ServiceCredentials(vcapServices);
    }
}
