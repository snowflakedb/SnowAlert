import {BaselinePayload, ConnectorPayload} from '../reducers/types';

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

export class Baseline {
  raw: BaselinePayload;
  title: string;
  baseline: string;
  description: string;
  options: any[];

  constructor(bl: BaselinePayload) {
    this.raw = bl;
    this.baseline = bl.baseline;
    this.options = bl.options.slice();
    this.title = (bl.docstring || 'title missing').replace(/\n.*/g, '');
    this.description = (bl.docstring || '').replace(/^[^\n]*\n/g, '');
  }
}
