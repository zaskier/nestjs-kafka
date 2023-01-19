import { Injectable } from '@nestjs/common';
import {
  Kafka,
  Consumer,
  ConsumerSubscribeTopics,
  ConsumerRunConfig,
} from 'kafkajs';

@Injectable()
export class ConsumerService {
  readonly kafka = new Kafka({
    brokers: ['localhost:9092'],
  });
  private readonly consmuer: Consumer[] = [];

  async consume(topic: ConsumerSubscribeTopics, config: ConsumerRunConfig) {
    const consumer = this.kafka.consumer({ groupId: 'nestjs-kafka' });
    await consumer.connect();
    await consumer.subscribe(topic);
    await consumer.run(config);
    this.consmuer.push(consumer);
  }

  async onApplicationShutdown() {
    for (const consmuer of this.consmuer) {
      await consmuer.disconnect();
    }
  }
}
