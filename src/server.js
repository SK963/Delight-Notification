const express = require('express');
const mongoose = require('mongoose');
const { Kafka, logLevel } = require('kafkajs');
const cors = require('cors');
const helmet = require('helmet');
const winston = require('winston');
const swaggerUi = require('swagger-ui-express');
const YAML = require('yamljs');
const path = require('path');
require('dotenv').config();

// ── Logger ──────────────────────────────────────────────────────────────────
const logger = winston.createLogger({
  level: process.env.LOG_LEVEL || 'info',
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.json()
  ),
  defaultMeta: { service: 'notification-service' },
  transports: [new winston.transports.Console()]
});

// ── Express App ─────────────────────────────────────────────────────────────
const app = express();
app.use(helmet());
app.use(cors());
app.use(express.json());

// ── Swagger UI ──────────────────────────────────────────────────────────────
const swaggerDoc = YAML.load(path.join(__dirname, 'openapi.yaml'));
app.use('/api-docs', swaggerUi.serve, swaggerUi.setup(swaggerDoc, {
  customCss: '.swagger-ui .topbar { display: none }',
  customSiteTitle: 'Notification Service – API Docs'
}));

// ── Mongoose Model ──────────────────────────────────────────────────────────
const notificationSchema = new mongoose.Schema(
  {
    orderId:   String,
    recipient: String,
    type:      String,
    status:    String,
    message:   String,
    sentAt:    Date
  },
  { timestamps: true }
);

const Notification = mongoose.model('Notification', notificationSchema);

// ── Kafka Consumer ──────────────────────────────────────────────────────────
const kafka = new Kafka({
  clientId: 'notification-service',
  brokers: (process.env.KAFKA_BROKERS || 'kafka:9092').split(','),
  logLevel: logLevel.WARN,
  retry: { initialRetryTime: 1000, retries: 10 }
});

const consumer = kafka.consumer({ groupId: 'notification-group' });
const TOPIC = 'order-events';

let lastEvent = null;

// ── Routes ──────────────────────────────────────────────────────────────────

// Health check
app.get('/health', (_req, res) => {
  res.json({ service: 'notification-service', status: 'ok', lastEvent });
});

// List all notifications
app.get('/api/notifications', async (req, res, next) => {
  try {
    const notifications = await Notification.find()
      .sort({ createdAt: -1 })
      .limit(100);
    res.json(notifications);
  } catch (err) {
    next(err);
  }
});

// Get notifications for a specific order
app.get('/api/notifications/:orderId', async (req, res, next) => {
  try {
    const notifications = await Notification.find({
      orderId: req.params.orderId
    });
    if (!notifications.length) {
      return res.status(404).json({
        error: { message: 'No notifications found for this order' }
      });
    }
    res.json(notifications);
  } catch (err) {
    next(err);
  }
});

// ── Global Error Handler ────────────────────────────────────────────────────
app.use((err, _req, res, _next) => {
  logger.error('Unhandled error', { error: err.message, stack: err.stack });
  res.status(500).json({
    error: { code: 'NOTIFICATION_ERROR', message: err.message }
  });
});

// ── Kafka Event Handler ────────────────────────────────────────────────────
async function handleOrderEvent(event) {
  const notification = await Notification.create({
    orderId:   event.data.orderId,
    recipient: event.data.customerEmail,
    type:      'ORDER_CONFIRMATION',
    status:    'SENT',
    message:   `Your Cake Delight order ${event.data.orderId} is confirmed! Total: $${event.data.total}`,
    sentAt:    new Date()
  });

  logger.info('Notification sent', {
    orderId: event.data.orderId,
    recipient: event.data.customerEmail,
    notificationId: notification._id
  });
}

// ── Start Server ────────────────────────────────────────────────────────────
async function start() {
  // Connect to MongoDB
  await mongoose.connect(process.env.MONGO_URI);
  logger.info('Connected to MongoDB');

  // Connect Kafka consumer with retry
  await consumer.connect();
  logger.info('Kafka consumer connected');

  // Retry subscribe – topic may not exist yet if no orders have been placed
  let subscribed = false;
  for (let attempt = 1; attempt <= 10; attempt++) {
    try {
      await consumer.subscribe({ topic: TOPIC, fromBeginning: false });
      subscribed = true;
      logger.info('Kafka consumer subscribed', { topic: TOPIC });
      break;
    } catch (err) {
      logger.warn(`Subscribe attempt ${attempt}/10 failed: ${err.message}`);
      if (attempt < 10) await new Promise(r => setTimeout(r, 3000));
    }
  }

  if (!subscribed) {
    logger.warn('Could not subscribe to topic yet – starting HTTP server anyway. Will retry on next restart.');
  }

  // Process incoming messages (only runs if subscribed)
  if (subscribed) {
    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        try {
          const event = JSON.parse(message.value.toString());
          lastEvent = event;

          logger.info('Received event', {
            eventType: event.eventType,
            orderId: event.data?.orderId,
            topic,
            partition,
            offset: message.offset
          });

          if (event.eventType === 'ORDER_COMPLETED') {
            await handleOrderEvent(event);
          }
        } catch (err) {
          logger.error('Error processing Kafka message', {
            error: err.message,
            topic,
            partition,
            offset: message.offset
          });
        }
      }
    });
  }

  const PORT = process.env.PORT || 3004;
  app.listen(PORT, () => logger.info(`notification-service ready on port ${PORT}`));
}

start().catch((err) => {
  logger.error('Failed to start notification-service', { error: err.message });
  process.exit(1);
});
