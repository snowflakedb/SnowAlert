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

  get resetCopy() {
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
    return this._raw.body == this._raw.savedBody;
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
      `  COMMENT='${this.title.replace(/'/g, "\\'")}'\n` +
      `AS\n` +
      this.subpolicies
        .map(sp => `  SELECT '${sp.title.replace(/'/g, "\\'")}' AS title\n` + `       , ${sp.condition} AS passing`)
        .join('\nUNION ALL\n') +
      `\n;\n`
    );
  }
}
