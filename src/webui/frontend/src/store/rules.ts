import * as stateTypes from '../reducers/types';
import * as _ from 'lodash';

function raise(e: string): never {
  throw e;
}

export class Subpolicy {
  i: number;
  title: string;
  passing?: boolean;
  condition: string;

  constructor(i: number) {
    this.i = i;
    this.title = '';
    this.passing = undefined;
    this.condition = '';
  }
}

const BLANK_POLICY = (viewName: string) =>
  `CREATE OR REPLACE VIEW rules.${viewName}_POLICY_DEFINITION COPY GRANTS\n` +
  `  COMMENT='Policy Title\n` +
  `description goes here'\n` +
  `AS\n` +
  `  SELECT 'subpolicy title' AS title\n` +
  `       , true AS passing\n` +
  `;`;

function stripComment(body: string): {rest: string; comment: string; viewName: string} {
  const vnameRe = /^CREATE OR REPLACE VIEW [^.]+.[^.]+.([^\s]+) ?COPY GRANTS\s*\n/im;
  const descrRe = /^\s*COMMENT='((?:\\'|[^'])*)'\s*\nAS\s*\n/gim;

  const vnameMatch = vnameRe.exec(body);

  if (!vnameMatch) {
    return {rest: body, comment: '', viewName: ''};
  }

  const vnameAfter = body.substr(vnameMatch[0].length);

  const descrMatch = descrRe.exec(vnameAfter) || raise('no descr match');
  const descrAfter = vnameAfter.substr(descrMatch[0].length);

  return {
    rest: descrAfter,
    comment: descrMatch[1].replace(/\\'/g, "'"),
    viewName: vnameMatch[1],
  };
}

function parseComment(comment: string): {summary: string; decorations: {[name: string]: string}} {
  const decorations: {[name: string]: string} = {};

  const summary = comment
    .split('\n')
    .map(line => {
      const match = line.match(/^\s*@([a-z-]+) (.*)$/i);
      if (match) {
        decorations[match[1]] = match[2];
        return undefined;
      } else {
        return line;
      }
    })
    .filter(x => x !== undefined)
    .join('\n');

  return {summary, decorations};
}

export abstract class SQLBackedRule {
  _raw: stateTypes.SnowAlertRule;

  isSaving: boolean;
  isEditing: boolean;
  isParsed: boolean;

  constructor(rule: stateTypes.SnowAlertRule) {
    this.raw = rule;
    this.isSaving = false;
    this.isEditing = false;
  }

  copy(toMerge: any) {
    return _.mergeWith(_.cloneDeep(this), toMerge, (a, b) => (_.isArray(a) ? b : undefined));
  }

  get raw() {
    return Object.assign({}, this._raw, this.isParsed ? {body: this.body} : undefined);
  }

  set raw(r: stateTypes.SnowAlertRule) {
    this._raw = r;
    try {
      this.load(r.body, r.results);
      this.isParsed = this.body === r.body;
      // if (!this.isParsed) {
      //   console.log('r.body', r.body)
      //   console.log('this.body', this.body)
      // }
    } catch (e) {
      // console.log(`error parsing >${r.body}< ${new Error(e)}`, e);
      this.isParsed = false;
    }
  }

  get type() {
    return this.raw.type;
  }

  get viewName(): string {
    return `${this._raw.title}_${this._raw.target}_${this._raw.type}`;
  }

  get rawTitle(): string {
    return this.raw.title
      .replace(/_/g, ' ')
      .toLowerCase()
      .replace(/\b[a-z]/g, c => c.toUpperCase());
  }

  get title(): string {
    return this.rawTitle;
  }

  get isSaved() {
    return this._raw.savedBody !== '';
  }

  get isEdited() {
    return this.raw.body !== this._raw.savedBody;
  }

  abstract load(body: string, results?: stateTypes.SnowAlertRule['results']): void;
  abstract get body(): string;
}

export class Policy extends SQLBackedRule {
  views: string;
  comment: string;
  subpolicies: Subpolicy[];

  static create() {
    const viewName = `PD_${Math.random()
      .toString(36)
      .substring(2)}`;
    return new Policy({
      target: 'POLICY',
      type: 'DEFINITION',
      title: viewName,
      results: [{PASSING: undefined, TITLE: 'subpolicy title'}],
      body: BLANK_POLICY(viewName),
      savedBody: '',
      isSaving: false,
      newTitle: '',
    });
  }

  copy() {
    return new Policy(this._raw);
  }

  get passing(): boolean {
    return this.subpolicies.reduce((prev, sp) => (sp.passing ? true : prev), true);
  }

  get title() {
    return this.comment.replace(/\n.*$/g, '');
  }

  set title(newTitle: string) {
    this.comment = this.comment.replace(/^.*?\n/, `${newTitle}\n`);
  }

  set summary(newDescription: string) {
    this.comment = this.comment.replace(/\n.*$/, `\n${newDescription}`);
  }

  get summary() {
    return this.comment.replace(/^.*?\n/g, '');
  }

  load(sql: string, results: stateTypes.SnowAlertRule['results']) {
    const vnameRe = /^CREATE OR REPLACE VIEW [^.]+.[^.]+.([^\s]+) ?COPY GRANTS\s*\n/m;
    const descrRe = /^  COMMENT='([^']+)'\nAS\n/gm;
    const subplRe = /^  SELECT '((?:\\'|[^'])+)' AS title\n       , ([^;]+?) AS passing$(?:\n;|\nUNION ALL\n)?/m;

    const vnameMatch = vnameRe.exec(sql) || raise('no vname match');
    const vnameAfter = sql.substr(vnameMatch[0].length);

    const descrMatch = descrRe.exec(vnameAfter) || raise('no descr match');
    const descrAfter = vnameAfter.substr(descrMatch[0].length);

    this.comment = descrMatch[1];
    this.subpolicies = [];

    let rest = descrAfter;
    let i = 0;

    do {
      const matchSubpl = subplRe.exec(rest) || raise(`no title match >${sql}|${rest}<`);
      rest = rest.substr(matchSubpl[0].length);

      this.subpolicies.push({
        i,
        passing: results ? results[i++].PASSING : false,
        title: matchSubpl[1].replace(/\\'/g, "'"),
        condition: matchSubpl[2],
      });
    } while (rest.replace(/\s/g, ''));
  }

  get isEdited() {
    return this.body !== this._raw.savedBody;
  }

  get body(): string {
    return (
      `CREATE OR REPLACE VIEW rules.${this.viewName} COPY GRANTS\n` +
      `  COMMENT='${this.comment.replace(/'/g, "\\'")}'\n` +
      `AS\n` +
      this.subpolicies
        .map(sp => `  SELECT '${sp.title.replace(/'/g, "\\'")}' AS title\n` + `       , ${sp.condition} AS passing`)
        .join('\nUNION ALL\n') +
      `\n;\n`
    );
  }
}

interface QueryFields {
  select: {
    [prop: string]: string;
  };
  from: string;
  enabled: boolean;
  where: string;
}

export class Query extends SQLBackedRule {
  fields: QueryFields;
  summary: string;
  tags: string[];

  get target() {
    return this.raw.target;
  }

  load(body: string) {
    function stripField(sql: string): {rest: string; field: string; value: string} | null {
      const match = sql.match(/^\s*(?:SELECT|,)\s*([\s\S]*?) AS (\w*)$/im);
      if (!match || sql.match(/^\s*FROM/i)) {
        // in case of sub-queries, FROM match needs to be explicit
        return null;
      } else {
        const [m, value, field] = match;
        return {
          rest: sql.substr(m.length),
          field,
          value,
        };
      }
    }

    function stripFrom(sql: string): {rest: string; from: string} {
      const [match, from] = sql.match(/^\s*FROM ([\S\s]*?)\s+^WHERE\s/im) || raise('no from');
      return {
        rest: sql.substr(match.length - 6),
        from,
      };
    }

    function stripWhere(sql: string): {enabled: boolean; where: string; more: boolean} {
      const [match, enabled, where] = sql.match(/^WHERE (1=[01]|)\s*(?:AND )?([\s\S]*?)\s*;$/im) || raise('err2');
      return {
        enabled: !(enabled === '1=0'),
        where,
        more: match.length !== sql.length,
      };
    }

    const fields = {
      select: {},
      from: '',
      enabled: false,
      where: '',
    };

    const query = stripComment(body);
    let {rest} = query;
    const {comment} = query;
    const {
      summary,
      decorations: {tags},
    } = parseComment(comment);
    this.tags = tags ? tags.split(', ') : [];
    this.summary = summary;

    let nextField = stripField(rest);
    if (!nextField) {
      throw new Error('err0');
    }
    do {
      const {field, value} = nextField;
      fields.select[field] = value.replace(/\n       /g, '\n');
      nextField = stripField(nextField.rest);
    } while (nextField);

    const afterFrom = stripFrom(rest);
    rest = afterFrom.rest;
    fields.from = afterFrom.from;

    const {enabled, where} = stripWhere(rest);
    fields.enabled = enabled;
    fields.where = where;
    this.fields = fields;
  }

  get title() {
    try {
      return this.fields.select.title.match(/^'(.*)'$/)![1];
    } catch (e) {
      return this.rawTitle;
    }
  }

  get body() {
    const tagsLine = this.tags.length ? `\n  @tags ${this.tags.join(', ')}` : '';
    const queryId = this.fields.select.query_id.replace(/'/g, '');
    const queryLine = `\n  @id ${queryId}`;

    return (
      `CREATE OR REPLACE VIEW rules.${this.viewName} COPY GRANTS\n` +
      `  COMMENT='${this.summary
        .replace(/'/g, "\\'")
        .replace(/^/gm, '  ')
        .substr(2)}` +
      `${queryLine}` +
      `${tagsLine}'\n` +
      `AS\n` +
      `SELECT ${Object.entries(this.fields.select)
        .map(([k, v]) => `${v.replace(/\n/g, '\n       ')} AS ${k}`)
        .join('\n     , ')}\n` +
      `FROM ${this.fields.from}\n` +
      `WHERE 1=${this.fields.enabled ? '1' : '0'}\n` +
      `  AND ${this.fields.where}\n;`
    );
  }
}

export class Suppression extends SQLBackedRule {
  conditions: string[];
  summary: string;
  from: string;
  tags: string[];

  get target() {
    return this.raw.target;
  }

  load(sql: string) {
    function stripStart(sql: string): {rest: string; from: string} | null {
      const headRe = /^SELECT (?:\*|id)\s+FROM ([\s\S]+)\s+WHERE suppressed IS NULL\n/im;
      const m = sql.match(headRe);
      return m ? {rest: sql.substr(m[0].length), from: m[1]} : null;
    }

    // function stripRule(sql: string): {rule: string; rest: string} | null {
    //   const ruleRe = /^(\s*(AND[\s\S]*)\s*)$/im;
    //   const m = sql.match(ruleRe);
    //   return m ? {rule: m[2], rest: sql.substr(m[1].length)} : null;
    // }

    const parsedComment = stripComment(sql);
    const {
      summary,
      decorations: {tags},
    } = parseComment(parsedComment.comment);
    const parsedStart = stripStart(parsedComment.rest) || raise('err0');
    const {rest} = parsedStart;
    const {from} = parsedStart;

    // hack until string array UI is ready
    const rulesString = rest.replace(/;\s*$/gm, '');
    this.conditions = [rulesString];

    // const r = stripRule(rulesString) || raise(`err1 >${rulesString}<`);
    // rest = r.rest;
    // let {rule} = r;
    // const conditions = [];
    // while (rest.length > 1) {
    //   conditions.push(rule);
    //   const afterRule = stripRule(rest) || raise(`err2.${conditions.length} >${rest}<`);
    //   rest = afterRule.rest;
    //   rule = afterRule.rule;
    // }

    this.tags = tags ? tags.split(', ') : [];
    this.summary = summary;
    this.from = from;
  }

  get title() {
    return this.isParsed ? this.summary.replace(/\n.*$/g, '') : this.raw.title;
  }

  set title(newTitle: string) {
    this.summary = this.summary.replace(/^[^\n]*/, newTitle);
  }

  get body() {
    const tagsLine = this.tags.length ? `\n  @tags ${this.tags.join(', ')}` : '';

    return (
      `CREATE OR REPLACE VIEW rules.${this.viewName} COPY GRANTS\n` +
      `  COMMENT='${this.summary
        .replace(/'/g, "\\'")
        .replace(/^/gm, '  ')
        .substr(2)}` +
      `${tagsLine}'\n` +
      `AS\n` +
      `SELECT id\n` +
      `FROM ${this.from}\n` +
      `WHERE suppressed IS NULL\n` +
      `${this.conditions[0]}` +
      `;`
    );
  }
}
