import {ConnectorPayload} from '../reducers/types';

export class Connector {
  raw: ConnectorPayload;
  name: string;
  description: string;
  options: any;
  finalize: boolean;
  secret: boolean;
  maskOnScreen: boolean;
  title: string;
  prompt: string;
  placeholder: string;

  constructor(payload: ConnectorPayload) {
    this.raw = payload;
    this.name = payload.connector;
    this.title = payload.docstring.replace(/\n.*/m, '');
    this.options = payload.options;
    this.finalize = payload.finalize;
    this.description = payload.docstring.replace(/[^\n]*\n/m, '');
    this.secret = payload.secret;
    this.maskOnScreen = payload.mask_on_screen;
    this.prompt = payload.prompt;
    this.placeholder = payload.placeholder;
  }
}
