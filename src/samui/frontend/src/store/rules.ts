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

const BLANK_POLICY = (view_name: string) =>
  `CREATE OR REPLACE VIEW x.y.${view_name}_POLICY_DEFINITION COPY GRANTS` +
  `  COMMENT='Policy Title` +
  `description goes here'` +
  `AS` +
  `  SELECT 'subpolicy title' AS title` +
  `       , true AS passing` +
  `;`;

function stripComment(sql: string): {sql: string; comment: string; view_name: string} {
  const vnameRe = /^CREATE OR REPLACE VIEW [^.]+.[^.]+.([^\s]+) ?COPY GRANTS\s*\n/m,
    descrRe = /^  COMMENT='((?:\\'|[^'])*)'\nAS\n/gm;

  const vnameMatch = vnameRe.exec(sql);

  if (!vnameMatch) {
    return {sql, comment: '', view_name: ''};
  }

  const vnameAfter = sql.substr(vnameMatch[0].length);

  const descrMatch = descrRe.exec(vnameAfter) || raise('no descr match'),
    descrAfter = vnameAfter.substr(descrMatch[0].length);

  return {
    sql: descrAfter,
    comment: descrMatch[1],
    view_name: vnameMatch[1],
  };
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

  set raw(r: stateTypes.SnowAlertRule) {
    this._raw = r;
    try {
      this.load(r.body, r.results);
      this.isParsed = this.body === r.body;
    } catch (e) {
      // console.log(`error parsing >${r.body}< ${e}`);
      this.isParsed = false;
    }
  }

  get view_name(): string {
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

  get raw() {
    return Object.assign({}, this._raw, this.isParsed ? {body: this.body} : undefined);
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

  set title(newTitle) {
    this.comment = this.comment.replace(/^.*?\n/, `${newTitle}\n`);
  }

  set summary(newDescription) {
    this.comment = this.comment.replace(/\n.*$/, `\n${newDescription}`);
  }

  get summary() {
    return this.comment.replace(/^.*?\n/g, '');
  }

  load(sql: string, results: stateTypes.SnowAlertRule['results']) {
    const vnameRe = /^CREATE OR REPLACE VIEW [^.]+.[^.]+.([^\s]+) ?COPY GRANTS\s*\n/m,
      descrRe = /^  COMMENT='([^']+)'\nAS\n/gm,
      subplRe = /^  SELECT '((?:\\'|[^'])+)' AS title\n       , ([^;]+?) AS passing$(?:\n;|\nUNION ALL\n)?/m;

    const vnameMatch = vnameRe.exec(sql) || raise('no vname match'),
      vnameAfter = sql.substr(vnameMatch[0].length);

    const descrMatch = descrRe.exec(vnameAfter) || raise('no descr match'),
      descrAfter = vnameAfter.substr(descrMatch[0].length);

    this.comment = descrMatch[1];
    this.subpolicies = [];

    var rest = descrAfter;
    var i = 0;

    do {
      var matchSubpl = subplRe.exec(rest) || raise(`no title match >${sql}|${rest}<`),
        rest = rest.substr(matchSubpl[0].length);

      this.subpolicies.push({
        i,
        passing: results ? results[i++].PASSING : false,
        title: matchSubpl[1].replace(/\\'/g, "'"),
        condition: matchSubpl[2],
      });
    } while (rest.replace(/\s/g, ''));
  }

  get body(): string {
    return (
      `CREATE OR REPLACE VIEW snowalert.rules.${this.view_name} COPY GRANTS\n` +
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

  load(sql: string) {
    function parseComment(comment: string): {summary: string; tags: string[]} {
      const summaryRe = /^([\s\S]*?)(?:\n  @tags (.*))?$/m;
      const summaryMatch = summaryRe.exec(comment);

      var summary = summaryMatch ? summaryMatch[1] : '';

      return {
        summary: summary
          .replace(/^  /gm, '')
          .replace(/\n$/, '')
          .replace(/\\'/g, "'"),
        tags: summaryMatch && summaryMatch[2] ? summaryMatch[2].split(', ') : [],
      };
    }

    function stripField(sql: string): {sql: string; field: string; value: string} | null {
      const match = sql.match(/^\s*(?:SELECT|,)\s*([\s\S]*?) AS (\w*)$/im);
      if (!match || sql.match(/^\s*FROM/i)) {
        // in case of sub-queries, FROM match needs to be explicit
        return null;
      } else {
        const [m, value, field] = match;
        return {
          sql: sql.substr(m.length),
          field,
          value,
        };
      }
    }

    function stripFrom(sql: string): {sql: string; from: string} {
      const [match, from] = sql.match(/^\s*FROM ([\S\s]*?)\s+^WHERE\s/im) || raise('no from');
      return {
        sql: sql.substr(match.length - 6),
        from,
      };
    }

    function stripWhere(sql: string): {enabled: boolean; where: string; more: boolean} {
      const [match, enabled, where] = sql.match(/^\s*WHERE (1=[01]|)\s*(?:AND )?([\s\S]*?)\s*;$/im) || raise('err2');
      return {
        enabled: !(enabled === '1=0'),
        where,
        more: match.length !== sql.length,
      };
    }

    var fields = {
      select: {},
      from: '',
      enabled: false,
      where: '',
    };

    var {comment, sql} = stripComment(sql);
    var {summary, tags} = parseComment(comment);
    this.summary = summary;
    this.tags = tags;

    var nextField = stripField(sql);
    if (!nextField) throw 'err0';
    do {
      var {sql, field, value} = nextField;
      fields.select[field] = value.replace(/\n       /g, '\n');
    } while ((nextField = stripField(sql)));

    var {sql, from} = stripFrom(sql);
    fields.from = from;

    var {enabled, where} = stripWhere(sql);
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
    var tagsLine = this.tags.length ? `\n  @tags ${this.tags.join(', ')}` : '';

    return (
      `CREATE OR REPLACE VIEW snowalert.rules.${this.view_name} COPY GRANTS\n` +
      `  COMMENT='${this.summary
        .replace(/'/g, "\\'")
        .replace(/^/gm, '  ')
        .substr(2)}` +
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

interface SuppressionFields {
  from: string;
  rulesString: string;
  rules: Array<string>;
}

export class Suppression extends SQLBackedRule {
  fields: SuppressionFields;
  tags: string[];
  rules: string[];

  load(sql: string) {
    function stripStart(sql: string): {rest: string; from: string} | null {
      const headRe = /^SELECT (?:\*|alert)\s+FROM ([\s\S]+)\s+WHERE suppressed IS NULL\s+/im;
      const m = sql.match(headRe);
      return m ? {rest: sql.substr(m[0].length), from: m[1]} : null;
    }

    function stripRule(sql: string): {rule: string; rest: string} | null {
      const ruleRe = /^(\s*(AND[\s\S]*?)\s*)(?:AND[\s\S]*|;$)/im;
      const m = sql.match(ruleRe);
      return m ? {rule: m[2], rest: sql.substr(m[1].length)} : null;
    }

    var {sql} = stripComment(sql);
    var {rest, from} = stripStart(sql) || raise('err0');
    const rulesString = rest.replace(/;\s*$/gm, ''); // hack until array UI ready
    var {rule, rest} = stripRule(rest) || raise('err1');
    var rules = [];
    while (rest.length > 1) {
      rules.push(rule);
      var {rule, rest} = stripRule(rest) || raise(`err2.${rules.length} >${rest}<`);
    }

    this.tags = [];
    this.rules = rules;

    rules.push(rule);
    this.fields = {rules, from, rulesString};
  }

  get body() {
    // const whereClauseLines = ['WHERE 1=1', 'AND suppressed IS NULL'].concat(fields.rules)
    return (
      `CREATE OR REPLACE VIEW snowalert.rules.${this.view_name} COPY GRANTS AS\n` +
      `SELECT *\n` +
      `FROM ${this.fields.from}\n` +
      `WHERE suppressed IS NULL\n` +
      `${this.fields.rulesString};`
      // `${whereClauseLines.join('\n  ')}\n;`
    );
  }
}
