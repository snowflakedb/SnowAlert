import {Reducer} from 'redux';
import * as RulesActions from '../actions/rules';
import {SnowAlertRule, SnowAlertRulesState, State} from './types';
import {Query, Policy, Subpolicy, Suppression} from '../store/rules';

export const initialState: SnowAlertRulesState = {
  currentRuleView: null,
  errorMessage: null,
  filter: '',
  isFetching: false,
  policies: [],
  queries: [],
  suppressions: [],
};

const alertQueryBody = (s: string, qid: string) => `CREATE OR REPLACE VIEW snowalert.rules.${s}_ALERT_QUERY COPY GRANTS
  COMMENT=''
AS
SELECT 'E' AS environment
     , ARRAY_CONSTRUCT('S') AS sources
     , 'Predicate' AS object
     , 'rule title' AS title
     , CURRENT_TIMESTAMP() AS event_time
     , CURRENT_TIMESTAMP() AS alert_time
     , 'S: Subject Verb Predicate at ' || alert_time AS description
     , 'Subject' AS actor
     , 'Verb' AS action
     , 'SnowAlert' AS detector
     , OBJECT_CONSTRUCT(*) AS event_data
     , 'low' AS severity
     , '${s}' AS query_name
     , '${qid}' AS query_id
FROM snowalert.data.\nWHERE 1=1\n  AND 2=2\n;`;

const violationQueryBody = (
  s: string,
  qid: string,
) => `CREATE OR REPLACE VIEW snowalert.rules.${s}_ALERT_QUERY COPY GRANTS
  COMMENT=''
AS
SELECT 'E' AS environment
     , 'Predicate' AS object
     , 'rule title' AS title
     , 'S: Subject state' AS description
     , current_timestamp() AS alert_time
     , OBJECT_CONSTRUCT(*) AS event_data
     , 'SnowAlert' AS detector
     , 'low' AS severity
     , '${s}' AS query_name
     , '${qid}' AS query_id
FROM snowalert.data.\nWHERE 1=1\n AND 2=2\n;`;

const alertSuppressionBody = (s: string) => `CREATE OR REPLACE VIEW snowalert.rules.${s}_ALERT_SUPPRESSION COPY GRANTS
SELECT *\nFROM snowalert.results.alerts\nWHERE suppressed IS NULL\nAND ...;`;

const violationSuppressionBody = (
  s: string,
) => `CREATE OR REPLACE VIEW snowalert.rules.${s}_VIOLATION_SUPPRESSION COPY GRANTS
SELECT * \nFROM snowalert.results.violations\nWHERE suppressed IS NULL\nAND ...;`;

const NEW_RULE_BODY = (type: SnowAlertRule['type'], target: SnowAlertRule['target'], s: string, qid: string) => {
  if (type === 'QUERY' && target === 'ALERT') {
    return alertQueryBody(s, qid);
  } else if (type === 'QUERY' && target === 'VIOLATION') {
    return violationQueryBody(s, qid);
  } else if (type === 'SUPPRESSION' && target === 'ALERT') {
    return alertSuppressionBody(s);
  } else if (type === 'SUPPRESSION' && target === 'VIOLATION') {
    return violationSuppressionBody(s);
  } else {
    throw 'maybe policies should be made here?';
  }
};

export const rules: Reducer<SnowAlertRulesState> = (
  state = initialState,
  action:
    | RulesActions.LoadRulesActions
    | RulesActions.EditRulesActions
    | RulesActions.ChangeFilterAction
    | RulesActions.NewRuleAction
    | RulesActions.UpdateRuleAction
    | RulesActions.DeleteRuleActions
    | RulesActions.NewRuleAction
    | RulesActions.RenameRuleActions,
) => {
  // const isView = (v: string | null, r: SnowAlertRule) => v && v === `${r.title}_${r.target}_${r.type}`;

  switch (action.type) {
    // loading rules
    case RulesActions.LOAD_SNOWALERT_RULES_REQUEST:
      return {
        ...state,
        isFetching: true,
      };
    case RulesActions.LOAD_SNOWALERT_RULES_SUCCESS:
      var rules: ReadonlyArray<SnowAlertRule> = action.payload;
      return {
        ...state,
        policies: rules.filter(r => r.target == 'POLICY').map(r => new Policy(Object.assign(r, {savedBody: r.body}))),
        queries: rules.filter(r => r.type == 'QUERY').map(r => new Query(Object.assign(r, {savedBody: r.body}))),
        suppressions: rules
          .filter(r => r.type == 'SUPPRESSION')
          .map(r => new Suppression(Object.assign(r, {savedBody: r.body}))),
        isFetching: false,
      };

    // saving rules
    case RulesActions.SAVE_RULE_REQUEST:
      return {
        ...state,
        queries: state.queries.map(q => (q.view_name === state.currentRuleView ? q.copy({isSaving: true}) : q)),
        suppressions: state.suppressions.map(
          q => (q.view_name === state.currentRuleView ? q.copy({isSaving: true}) : q),
        ),
        policies: state.policies.map(
          p => (p.view_name == state.currentRuleView ? Object.assign(p, {isSaving: true}) : p),
        ),
      };

    case RulesActions.SAVE_RULE_SUCCESS:
      const {target: savedTarget, type: savedType, title: savedTitle} = action.payload;
      const savedView = `${savedTitle}_${savedTarget}_${savedType}`;
      return {
        ...state,
        policies: state.policies.map(p => (savedView !== p.view_name ? p : new Policy(action.payload))),
        queries: state.queries.map(p => (savedView !== p.view_name ? p : new Query(action.payload))),
        suppressions: state.suppressions.map(p => (savedView !== p.view_name ? p : new Suppression(action.payload))),
      };

    case RulesActions.SAVE_RULE_FAILURE:
      var {rule, message} = action.payload;
      var viewName = `${rule.title}_${rule.target}_${rule.type}`;
      alert(`SAVE_RULE_FAILURE ${message}`);
      return {
        ...state,
        queries: state.queries.map(q => (q.view_name === viewName ? q.copy({isSaving: false}) : q)),
        suppressions: state.suppressions.map(s => (s.view_name === viewName ? s.copy({isSaving: false}) : s)),
      };

    // update_title
    case RulesActions.UPDATE_POLICY_TITLE:
      var {viewName, newTitle} = action.payload;
      return {
        ...state,
        policies: state.policies.map(p => (viewName !== p.view_name ? p : Object.assign(p, {title: newTitle}))),
      };

    // update_title
    case RulesActions.UPDATE_POLICY_DESCRIPTION:
      var {viewName, newDescription} = action.payload;
      return {
        ...state,
        policies: state.policies.map(
          p => (viewName !== p.view_name ? p : Object.assign(p, {description: newDescription})),
        ),
      };

    // updating which rule is selected
    case RulesActions.CHANGE_CURRENT_RULE:
      return {
        ...state,
        currentRuleView: action.payload,
      };

    // update a rule
    case RulesActions.UPDATE_RULE:
      var {ruleViewName, rule: r} = action.payload;
      return {
        ...state,
        queries: state.queries.map(q => (q.view_name === ruleViewName ? r : q)),
      };

    // updating which rule is being edited
    case RulesActions.EDIT_RULE:
      return {
        ...state,
        policies: state.policies.map(
          p => (p.view_name == state.currentRuleView ? Object.assign(p, {isEditing: true}) : p),
        ),
      };

    // revert rule when "cancel" button is clikced
    case RulesActions.REVERT_RULE:
      var policy = action.payload;
      return {
        ...state,
        policies: state.policies
          .filter(r => r.isSaved)
          .map(r => (state.currentRuleView === r.view_name ? policy.copy() : r)),
      };

    // delete subpolicy
    case RulesActions.DELETE_SUBPOLICY:
      var {ruleTitle, i} = action.payload;
      return {
        ...state,
        policies: state.policies.map(
          p =>
            p.view_name != ruleTitle
              ? p
              : Object.assign(p, {subpolicies: p.subpolicies.slice(0, i).concat(p.subpolicies.slice(i + 1))}),
        ),
      };

    // add subpolicy
    case RulesActions.ADD_SUBPOLICY:
      var {ruleTitle} = action.payload;
      return {
        ...state,
        policies: state.policies.map(
          p =>
            p.view_name != ruleTitle
              ? p
              : Object.assign(p, {subpolicies: p.subpolicies.concat(new Subpolicy(p.subpolicies.length))}),
        ),
      };

    // add subpolicy
    case RulesActions.ADD_POLICY:
      var p = Policy.create();
      p.isEditing = true;
      return {
        ...state,
        policies: [p].concat(state.policies),
        currentRuleView: p.view_name,
      };

    // add subpolicy
    case RulesActions.EDIT_SUBPOLICY:
      var {ruleTitle, i, change} = action.payload;
      return {
        ...state,
        policies: state.policies.map(
          p =>
            p.view_name != ruleTitle
              ? p
              : Object.assign(p, {
                  subpolicies: p.subpolicies
                    .slice(0, i)
                    .concat(Object.assign({}, p.subpolicies[i], change))
                    .concat(p.subpolicies.slice(i + 1)),
                }),
        ),
      };

    // updating which rule is selected
    case RulesActions.NEW_RULE:
      var {ruleType, ruleTarget} = action.payload,
        qid = Math.random()
          .toString(36)
          .substring(2)
          .toUpperCase(),
        title = `${ruleTarget.substr(0, 1)}${ruleType.substr(0, 1)}_${qid}`,
        newRule = {
          target: ruleTarget,
          type: ruleType,
          title: title,
          body: NEW_RULE_BODY(ruleType, ruleTarget, title, qid),
          savedBody: '',
          isSaving: false,
          isEditing: false,
          newTitle: null,
        };

      switch (ruleType) {
        case 'QUERY':
          return {
            ...state,
            currentRuleView: `${title}_${ruleTarget}_${ruleType}`,
            queries: state.queries.concat([new Query(newRule)]),
          };

        case 'SUPPRESSION':
          return {
            ...state,
            currentRuleView: `${title}_${ruleTarget}_${ruleType}`,
            suppressions: state.suppressions.concat([new Suppression(newRule)]),
          };

        default:
          return {
            ...state,
            currentRuleView: null,
          };
      }

    // updating rule body
    case RulesActions.CHANGE_CURRENT_RULE_BODY:
      const newBody = action.payload;
      return {
        ...state,
        queries: state.queries.map(q => (q.view_name === state.currentRuleView ? q.copy({raw: {body: newBody}}) : q)),
        suppressions: state.suppressions.map(
          s => (s.view_name === state.currentRuleView ? s.copy({raw: {body: newBody}}) : s),
        ),
      };

    // updating filter
    case RulesActions.CHANGE_CURRENT_FILTER:
      return {
        ...state,
        filter: action.payload,
      };

    // tag filter
    case RulesActions.ADD_TAG_FILTER:
      return {
        ...state,
        filter: `${state.filter} tag[${action.payload}]`,
      };

    case RulesActions.REMOVE_TAG_FILTER:
      return {
        ...state,
        filter: state.filter.replace(` tag[${action.payload}]`, ''),
      };

    // deleting rule
    case RulesActions.DELETE_RULE_REQUEST:
      return {
        ...state,
      };
    case RulesActions.DELETE_RULE_SUCCESS:
      return {
        ...state,
        currentRuleView: '',
        queries: state.queries.filter(q => q.view_name !== action.payload),
        suppressions: state.suppressions.filter(s => s.view_name !== action.payload),
      };
    case RulesActions.DELETE_RULE_FAILURE:
      var {rule, message} = action.payload;
      alert(`RULE_DELETION_FAILURE ${message}`);
      return {
        ...state,
      };

    // renaming rules
    case RulesActions.RENAME_RULE_REQUEST:
      return {
        ...state,
      };
    case RulesActions.RENAME_RULE_SUCCESS:
      var rule = action.payload;
      if (rule.newTitle != null) {
        newTitle = rule.newTitle;
      }
      return {
        ...state,
        currentRuleView: `${rule.newTitle}_${rule.target}_${rule.type}`,
      };
    case RulesActions.RENAME_RULE_FAILURE:
      var {rule, message} = action.payload;
      alert(`RULE_RENAMING_FAILURE ${message}`);
      return {
        ...state,
      };
  }
  return state;
};

export const getRules = (state: State) => {
  return state.rules;
};
