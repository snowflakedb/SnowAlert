import {ConnectorPayload} from '../reducers/types';

export class Connector {
  raw: ConnectorPayload;
  name: string;
  description: string;
  options: any;
  finalize: boolean;
  secret: boolean;
  prompt: string;

  constructor(payload: ConnectorPayload) {
    this.raw = payload;
    this.name = payload.connector;
    this.options = payload.options;
    this.finalize = payload.finalize;
    this.description = payload.docstring;
    this.secret = payload.secret;
    this.prompt = payload.prompt;
  }
}
