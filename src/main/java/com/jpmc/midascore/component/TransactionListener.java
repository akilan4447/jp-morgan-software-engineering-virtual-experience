package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.foundation.Balance;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.UserRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

@Component
public class TransactionListener {

    private static final Logger logger = LoggerFactory.getLogger(TransactionListener.class);

    private final UserRepository userRepository;
    private final DatabaseConduit databaseConduit;
    private final RestTemplate restTemplate;
    private final String incentiveApiUrl;

    public TransactionListener(
            UserRepository userRepository,
            DatabaseConduit databaseConduit,
            RestTemplateBuilder restTemplateBuilder,
            @Value("${general.incentive-api-url}") String incentiveApiUrl) {
        this.userRepository = userRepository;
        this.databaseConduit = databaseConduit;
        this.restTemplate = restTemplateBuilder.build();
        this.incentiveApiUrl = incentiveApiUrl;
    }

    @KafkaListener(topics = "${general.kafka-topic}", groupId = "${spring.kafka.consumer.group-id}")
    public void listen(Transaction transaction) {
        logger.info("Received transaction: {}", transaction);

        // Validate sender exists
        UserRecord sender = userRepository.findById(transaction.getSenderId());
        if (sender == null) {
            logger.warn("Sender not found: {}. Transaction rejected.", transaction.getSenderId());
            return;
        }

        // Validate recipient exists
        UserRecord recipient = userRepository.findById(transaction.getRecipientId());
        if (recipient == null) {
            logger.warn("Recipient not found: {}. Transaction rejected.", transaction.getRecipientId());
            return;
        }

        // Validate sender has sufficient balance
        if (sender.getBalance() < transaction.getAmount()) {
            logger.warn("Insufficient balance for sender: {}. Balance: {}, Required: {}",
                    sender.getName(), sender.getBalance(), transaction.getAmount());
            return;
        }

        // Apply the core transaction
        sender.setBalance(sender.getBalance() - transaction.getAmount());
        recipient.setBalance(recipient.getBalance() + transaction.getAmount());

        // Fetch incentive bonus from external API
        float incentive = fetchIncentive(transaction);
        if (incentive > 0) {
            recipient.setBalance(recipient.getBalance() + incentive);
            logger.info("Incentive of {} applied to recipient {}", incentive, recipient.getName());
        }

        databaseConduit.save(sender);
        databaseConduit.save(recipient);

        TransactionRecord transactionRecord = new TransactionRecord(sender, recipient, transaction.getAmount(), incentive);
        databaseConduit.save(transactionRecord);

        logger.info("Transaction applied. Sender {} new balance: {}, Recipient {} new balance: {}",
                sender.getName(), sender.getBalance(), recipient.getName(), recipient.getBalance());
    }

    private float fetchIncentive(Transaction transaction) {
        try {
            String url = incentiveApiUrl + "/incentive";
            Balance incentiveResponse = restTemplate.postForObject(url, transaction, Balance.class);
            if (incentiveResponse != null) {
                return incentiveResponse.getAmount();
            }
        } catch (Exception e) {
            logger.warn("Could not fetch incentive from API: {}", e.getMessage());
        }
        return 0f;
    }
}
