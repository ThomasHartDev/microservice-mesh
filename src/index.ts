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

export {
  SERVICE_IMAGES,
  checkDockerignore,
  checkImagePolicy,
  isDockerignored,
  parseDockerfile,
  parseDockerignore,
  type Finding,
  type IgnorePattern,
  type Instruction,
  type Language,
  type ParsedDockerfile,
  type ServiceImage,
  type Stage,
} from './images.js'

export {
  MemoryPaymentStore,
  PAYMENT_CHARGED_ROUTING_KEY,
  PAYMENT_FAILED_ROUTING_KEY,
  PaymentsService,
  reserveFingerprint,
} from './payments.js'

export type {
  Outcome,
  Payment,
  ReserveRequest,
} from './payments.js'

export {
  InventoryService,
  INVENTORY_FAILED_ROUTING_KEY,
  INVENTORY_RESERVED_ROUTING_KEY,
  MemoryInventoryStore,
  aggregateLines,
  available,
} from './inventory.js'

export {
  MemorySagaStore,
  ORDER_CANCELLED_ROUTING_KEY,
  ORDER_COMPLETED_ROUTING_KEY,
  OrderSagaOrchestrator,
  compensationsFor,
} from './saga.js'

export type {
  Compensation,
  SagaInstance,
} from './saga.js'
