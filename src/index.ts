export const VERSION = '0.1.0'

export {
  CATALOG,
  createEnvelope,
  getCatalogEntry,
  listMessageTypes,
  parseEnvelope,
  payloadSchemas,
  validate,
  type CatalogEntry,
  type JsonObject,
  type JsonSchema,
  type JsonValue,
  type MessageEnvelope,
  type MessageKind,
  type MessageType,
  type ServiceName,
  type ValidationError,
  type ValidationResult,
} from './contracts.js'

export {
  MemoryOrderStore,
  MemoryPublisher,
  ORDER_CREATED_ROUTING_KEY,
  OrdersService,
  defaultClock,
  requestFingerprint,
  totalCents,
} from './orders.js'

export type {
  Clock,
  Currency,
  HandleOutcome,
  LineItem,
  Order,
  OrderRecord,
  Publisher,
} from './orders.js'

export {
  ClosedError,
  MAX_DELIVER,
  SubjectError,
  createMemoryBroker,
  matchSubject,
  publishEnvelope,
  validSubject,
  type Broker,
  type Delivery,
} from './broker.js'
