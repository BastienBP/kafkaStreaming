import express from 'express';
import { KafkaStream, kafkaClient, snowflake, Table} from '../helpers/index';
import kafka from 'kafka-node';
import Check from 'express-validator/check';
import Filter from 'express-validator/filter';
import Promise from 'bluebird';
import HttpError from 'http-errors';


const router = express.Router();

const { matchedData } = Filter;
const { validationResult, query, param } = Check;

router
  .route('/')
  .get([
    query('time')
      .isIn(['day', 'month', 'year'])
      .optional()
  ], (req, res, next) => {
    (async () => {
      const errors = validationResult(req);
      if (!errors.isEmpty()) {
        throw new HttpError.BadRequest({ errors: errors.mapped() });
      }

      let { time = 'all' } = matchedData(req);


      const stream = KafkaStream.getKStream(`sentiments_${time}`);
      const messages = []
      stream.forEach(message => {
        messages.push(message);
      });

      return res.json(messages);
    })().catch(next);
  });


router
  .route('/users')
  .get([
    query('time')
      .isIn(['day', 'month', 'year'])
      .optional()
  ], (req, res, next) => {
    (async () => {
      const errors = validationResult(req);
      if (!errors.isEmpty()) {
        throw new HttpError.BadRequest({ errors: errors.mapped() });
      }

      let { time = 'all' } = matchedData(req);

      const topicName = `sentiments_${time}_user`;
      const table = new Table(topicName);
      await table.init();
      table.on('done', messages => {
        res.json(messages);
      });

      table.start();
    })().catch(next);
  });


router
  .route('/users/:userId')
  .get([
    param('userId')
      .optional(),
    query('time')
      .isIn(['day', 'month', 'year'])
      .optional()
  ], (req, res, next) => {
    (async () => {
      const errors = validationResult(req);
      if (!errors.isEmpty()) {
        throw new HttpError.BadRequest({ errors: errors.mapped() });
      }

      let { userId, time = 'all' } = matchedData(req);

      const stream = KafkaStream.getKStream(`sentiments_${time}${userId ? userId : ''}`);

      const messages = [];
      stream.forEach(message => {
        messages.push(message);
      });

      return res.json(messages);
    })().catch(next);
  });


export default router;