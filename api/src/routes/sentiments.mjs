import express from 'express';
import { Table } from '../helpers/index';
import Check from 'express-validator/check';
import Filter from 'express-validator/filter';
import HttpError from 'http-errors';
import moment from 'moment';


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

      const topicName = `sentiments_${time}`;
      const table = new Table(topicName);
      await table.init();
      table.on('done', messages => {
        if (!messages.length) {
          return res.json([]);
        }

        if (time === 'all') {
          return res.json(messages);
        }

        const date =  dateHandler(messages[0].value.date, time);

        return res.json(messages.map(message => {
          message.value.date = date;
          return message.value;
        }));
      });

      table.start();
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
        if (!messages.length) {
          return res.json([]);
        }

        if (time === 'all') {
          return res.json(messages);
        }

        const date =  dateHandler(messages[0].value.date, time);

        return res.json(messages.map(message => {
          message.value.date = date;
          return {[message.key]: message.value};
        }));
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

      const topicName = `sentiments_${time}_user`;
      const table = new Table(topicName);
      await table.init();
      table.on('done', messages => {
        if (!messages.length) {
          return res.json([]);
        }

        const userMessage = messages.find(message => message.key === userId);

        if (time === 'all') {
          return res.json(userMessage);
        }

        userMessage.value.date = dateHandler(userMessage.value.date, time)
        return res.json(userMessage);
      });

      table.start();
    })().catch(next);
  });


function dateHandler(timestamp, timeUnit) {
  timestamp = timestamp / 1000000000;
  if (timeUnit === 'day') {
    return moment.unix(timestamp).format('DD/MM/YYYY');
  }

  if (timeUnit === 'month') {
    return moment.unix(timestamp).format('MM/YYYY');
  }

  if (timeUnit === 'year') {
    return moment.unix(timestamp).format('YYYY');
  }
}

export default router;