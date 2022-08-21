/*
 * Copyright 2022 Shaburov Oleg
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kakafka.client;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SslConfigs;

import java.nio.file.Path;
import java.util.Properties;

import static org.apache.kafka.clients.CommonClientConfigs.RETRIES_CONFIG;
import static org.apache.kafka.clients.CommonClientConfigs.SECURITY_PROTOCOL_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.*;
import static org.apache.kafka.common.config.SaslConfigs.SASL_JAAS_CONFIG;
import static org.apache.kafka.common.config.SaslConfigs.SASL_MECHANISM;
import static org.apache.kafka.common.config.SslConfigs.*;

/**
 * @author Oleg Shaburov (shaburov.o.a@gmail.com)
 * Created: 21.08.2022
 */
public class KakafkaProperties extends Properties {

    public KakafkaProperties(String... bootstrapServerHosts) {
        put(BOOTSTRAP_SERVERS_CONFIG, String.join("," , bootstrapServerHosts));
        put(ACKS_CONFIG, "all");
        put(RETRIES_CONFIG, 0);
        put(BATCH_SIZE_CONFIG, 16384);
        put(LINGER_MS_CONFIG, 0);
        put(BUFFER_MEMORY_CONFIG, 33554432);
        put(SASL_MECHANISM, "");
        put(RECONNECT_BACKOFF_MS_CONFIG, "1000");
    }

    public KakafkaProperties withPlainSASL(final String username, final String password) {
        put(SASL_MECHANISM, "PLAIN");
        put(SASL_JAAS_CONFIG, "org.apache.kafka.common.security.plain.PlainLoginModule required " +
                              "username=\"" + username + "\" " +
                              "password=\"" + password + "\";");
        return this;
    }

    /**
     * @param withHttps               - see {@link CommonClientConfigs#SECURITY_PROTOCOL_DOC}
     * @param ignoreCertificateErrors - see {@link SslConfigs#SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_DOC}
     * @return this
     */
    public KakafkaProperties withHttps(final boolean withHttps, final boolean ignoreCertificateErrors) {
        put(SECURITY_PROTOCOL_CONFIG, withHttps ? "SASL_SSL" : "");
        put(SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, ignoreCertificateErrors ? "" : "https");
        return this;
    }

    public KakafkaProperties withTLSAuthByPKCS12KeyTrustStore(final Path keystore,
                                                              final String keystorePassword,
                                                              final String keystorePrivateKeyPassword,
                                                              final Path truststore,
                                                              final String truststorePassword) {
        return withTLSAuthByKeyTrustStore("PKCS12", keystore, keystorePassword, keystorePrivateKeyPassword,
                "PKCS12", truststore, truststorePassword);
    }

    public KakafkaProperties withTLSAuthByKeyTrustStore(final String userKeystoreType,
                                                        final Path userKeystorePath,
                                                        final String userKeystorePassword,
                                                        final String userKeystorePrivateKeyPassword,
                                                        final String serverCATruststoreType,
                                                        final Path serverTruststorePath,
                                                        final String serverTruststorePassword) {
        put(SECURITY_PROTOCOL_CONFIG, "SSL");
        put(SSL_KEYSTORE_TYPE_CONFIG, userKeystoreType);
        put(SSL_KEYSTORE_LOCATION_CONFIG, userKeystorePath.toAbsolutePath());
        put(SSL_KEYSTORE_PASSWORD_CONFIG, userKeystorePassword);
        put(SSL_KEY_PASSWORD_CONFIG, userKeystorePrivateKeyPassword);
        put(SSL_TRUSTSTORE_TYPE_CONFIG, serverCATruststoreType);
        put(SSL_TRUSTSTORE_LOCATION_CONFIG, serverTruststorePath.toAbsolutePath());
        put(SSL_TRUSTSTORE_PASSWORD_CONFIG, serverTruststorePassword);
        return this;
    }

    public KakafkaProperties withTLSAuthByKeyPair(final String userKeystoreType,
                                                  final Path userCrtPath,
                                                  final Path userKeyPath,
                                                  final String userKeyPassword,
                                                  final String serverCrtType,
                                                  final Path serverCrtPath) {
        put(SSL_KEYSTORE_TYPE_CONFIG, userKeystoreType);
        put(SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG, userCrtPath.toAbsolutePath());
        put(SSL_KEYSTORE_KEY_CONFIG, userKeyPath.toAbsolutePath());
        put(SSL_KEY_PASSWORD_CONFIG, userKeyPassword);
        put(SSL_TRUSTSTORE_TYPE_CONFIG, serverCrtType);
        put(SSL_TRUSTSTORE_CERTIFICATES_CONFIG, serverCrtPath);
        return this;
    }

    public KakafkaProperties withTLSAuthByPEMKeyPair(final String userCrt,
                                                     final String userKey,
                                                     final String userKeystorePrivateKeyPassword,
                                                     final String serverCrt) {
        put(SSL_KEYSTORE_TYPE_CONFIG, "PEM");
        put(SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG, userCrt);
        put(SSL_KEYSTORE_KEY_CONFIG, userKey);
        put(SSL_KEY_PASSWORD_CONFIG, userKeystorePrivateKeyPassword);
        put(SSL_TRUSTSTORE_TYPE_CONFIG, "PEM");
        put(SSL_TRUSTSTORE_CERTIFICATES_CONFIG, serverCrt);
        return this;
    }

}
