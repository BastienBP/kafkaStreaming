import EventEmitter from 'events';
import Promise from 'bluebird';
import kafka from 'kafka-node';
import { kafkaClient, snowflake } from './index';

export default class Table extends EventEmitter {
  constructor(topicName) {
    super();
    this.topicName = topicName;
    this._consumer = null;
    this._offset = null;
    this.offsets = null;
    this.nMessages = 0;
    this.messages = [];
  }

  async init() {
    this._consumer = new kafka.Consumer(kafkaClient, [{ topic: this.topicName , offset: 0, partition: 0}], {groupId: await snowflake.getId(), fromOffset: 'latest'});
    this._offset = Promise.promisifyAll(new kafka.Offset(kafkaClient));
    this.offsets = await this._offset.fetchAsync([{
      topic: this.topicName,
      partition: 0,
      time: -1
    }]);
    this.nMessages = this.offsets[this.topicName]['0'][0];
  }

  push(message) {
    if (this.messages.length >= this.nMessages) return false;
    this.messages.push(message);
    if (this.nMessages === this.messages.length) {
      this._consumer.close();
      this.emit('done', this.messages.map(message => {
        message.value = JSON.parse(message.value);
        return message;
      }));
    }
  }

  start() {
    this._consumer.on('message', message => {
      this.push(message);
    });
  }
}