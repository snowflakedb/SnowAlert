import * as stateTypes from '../reducers/types';

function raise(e: string): never {
  throw e;
}

// function matchAll(regexp: RegExp, s: string): string[][] {
//   var matches: string[][] = [];
//   s.replace(regexp, (...args) => {
//     matches.push(args);
//     return '';
//   });
//   return matches;
// };

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
  `CREATE OR REPLACE VIEW x.y.${view_name}_POLICY_DEFINITION COPY GRANTS
  COMMENT='Policy Title
description goes here'
AS
  SELECT 'subpolicy title' AS title
       , true AS passing
;`;

export class Policy {
  _raw: stateTypes.SnowAlertRule;
  views: string;
  comment: string;
  isSaving: boolean;
  isEditing: boolean;
  subpolicies: Subpolicy[];

  constructor(rule: stateTypes.SnowAlertRule) {
    this.raw = rule;
    this.isSaving = false;
    this.isEditing = false;
  }

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

  get copy() {
    return new Policy(this._raw);
  }

  get raw() {
    return Object.assign({}, this._raw, {body: this.body});
  }

  set raw(r) {
    this._raw = r;
    this.load_body(r.body, r.results);
  }

  get isSaved() {
    return this._raw.savedBody !== '';
  }

  get isEdited() {
    return this._raw.body !== this._raw.savedBody;
  }

  get view_name(): string {
    return this._raw.title + '_POLICY_DEFINITION';
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

  set description(newDescription) {
    this.comment = this.comment.replace(/\n.*$/, `\n${newDescription}`);
  }

  get description() {
    return this.comment.replace(/^.*?\n/g, '');
  }

  load_body(sql: string, results: stateTypes.SnowAlertRule['results']) {
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
