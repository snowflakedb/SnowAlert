import {Reducer} from 'redux';
import * as RulesActions from '../actions/rules';
import {SnowAlertRule, SnowAlertRulesState, State} from './types';
import {Policy, Subpolicy} from '../store/rules';

export const initialState: SnowAlertRulesState = {
  currentRuleView: null,
  errorMessage: null,
  filter: null,
  isFetching: false,
  rules: [],
  policies: [],
};

const alertQueryBody = (s: string) => `SELECT 'E' AS environment
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
     , '${Math.random()
       .toString(36)
       .substring(2)}' AS query_id
FROM snowalert.data.\nWHERE 1=1\n  AND 2=2\n;`;

const violationQueryBody = (s: string) => `SELECT 'E' AS environment
     , 'Predicate' AS object
     , 'rule title' AS title
     , 'S: Subject state' AS description
     , current_timestamp() AS alert_time
     , OBJECT_CONSTRUCT(*) AS event_data
     , 'SnowAlert' AS detector
     , 'low' AS severity
     , '${s}' AS query_name
     , '${Math.random()
       .toString(36)
       .substring(2)}' AS query_id
FROM snowalert.data.\nWHERE 1=1\n AND 2=2\n;`;

const alertSuppressionBody = (s: string) =>
  `SELECT *\nFROM snowalert.results.alerts\nWHERE suppressed IS NULL\nAND ...;`;

const violationSuppressionBody = (s: string) =>
  `SELECT * \nFROM snowalert.results.violations\nWHERE suppressed IS NULL\nAND ...;`;

const NEW_RULE_BODY = (type: SnowAlertRule['type'], target: SnowAlertRule['target'], s: string) => {
  if (type === 'QUERY' && target === 'ALERT') {
    return alertQueryBody(s);
  } else if (type === 'QUERY' && target === 'VIOLATION') {
    return violationQueryBody(s);
  } else if (type === 'SUPPRESSION' && target === 'ALERT') {
    return alertSuppressionBody(s);
  } else {
    return violationSuppressionBody(s);
  }
};

export const rules: Reducer<SnowAlertRulesState> = (
  state = initialState,
  action:
    | RulesActions.LoadRulesActions
    | RulesActions.EditRulesActions
    | RulesActions.ChangeFilterAction
    | RulesActions.NewRuleAction
    | RulesActions.DeleteRuleActions
    | RulesActions.NewRuleAction
    | RulesActions.RenameRuleActions
    | RulesActions.UpdateInterimTitleAction,
) => {
  const isView = (v: string | null, r: SnowAlertRule) => v && v == `${r.title}_${r.target}_${r.type}`;

  switch (action.type) {
    // loading rules
    case RulesActions.LOAD_SNOWALERT_RULES_REQUEST:
      return {
        ...state,
        isFetching: true,
      };
    case RulesActions.LOAD_SNOWALERT_RULES_SUCCESS:
      return {
        ...state,
        rules: action.payload.filter(r => r.target != 'POLICY').map(r => Object.assign(r, {savedBody: r.body})),
        policies: action.payload.filter(r => r.target == 'POLICY').map(r => new Policy(r)),
        isFetching: false,
      };

    // saving rules
    case RulesActions.SAVE_RULE_REQUEST:
      return {
        ...state,
        rules: state.rules.map(r => (isView(state.currentRuleView, r) ? Object.assign(r, {isSaving: true}) : r)),
      };

    case RulesActions.SAVE_RULE_SUCCESS:
      const {target: savedTarget, type: savedType, title: savedTitle, savedBody} = action.payload;
      const savedView = `${savedTitle}_${savedTarget}_${savedType}`;
      return {
        ...state,
        policies: state.policies.map(p => (savedView !== p.view_name ? p : new Policy(action.payload))),
        rules: state.rules.map(
          r =>
            !isView(savedView, r)
              ? r
              : Object.assign(r, {isSaving: false, isEditing: false, body: savedBody, savedBody: savedBody}),
        ),
      };

    case RulesActions.SAVE_RULE_FAILURE:
      var {rule, message} = action.payload;
      const viewName = `${rule.title}_${rule.target}_${rule.type}`;
      alert(`SAVE_RULE_FAILURE ${message}`);
      return {
        ...state,
        rules: state.rules.map(r => (isView(viewName, r) ? Object.assign(r, {isSaving: false}) : r)),
      };

    // updating rule title
    case RulesActions.CHANGE_TITLE:
      var {rule, newTitle} = action.payload;
      newTitle = newTitle
        .replace(/[\s_]+/g, '_')
        .toUpperCase()
        .replace(/[^0-9A-Z_]/g, '');
      return {
        ...state,
        rules: state.rules.map(r => (isView(state.currentRuleView, r) ? Object.assign(r, {title: newTitle}) : r)),
        currentRuleView: `${rule.title}_${rule.target}_${rule.type}`,
      };

    // updating which rule is selected
    case RulesActions.CHANGE_CURRENT_RULE:
      return {
        ...state,
        currentRuleView: action.payload,
      };

    // updating which rule is being edited
    case RulesActions.EDIT_RULE:
      return {
        ...state,
        policies: state.policies.map(
          p => (p.view_name == state.currentRuleView ? Object.assign(p, {isEditing: true}) : p),
        ),
        rules: state.rules.map(r => (isView(state.currentRuleView, r) ? Object.assign(r, {isEditing: true}) : r)),
      };

    // revert rule when "cancel" button is clikced
    case RulesActions.REVERT_RULE:
      var policy = action.payload;
      return {
        ...state,
        policies: state.policies.map(r => (state.currentRuleView == r.view_name ? policy.resetCopy : r)),
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
        title = `RULE_NUMBER_${state.rules.length + 1}`;

      return {
        ...state,
        currentRuleView: `${title}_${ruleTarget}_${ruleType}`,
        rules: state.rules.concat([
          {
            target: ruleTarget,
            type: ruleType,
            title: title,
            body: NEW_RULE_BODY(ruleType, ruleTarget, title),
            savedBody: '',
            isSaving: false,
            isEditing: false,
            newTitle: null,
          },
        ]),
      };

    // updating rule body
    case RulesActions.CHANGE_CURRENT_RULE_BODY:
      const newBody = action.payload;
      return {
        ...state,
        rules: state.rules.map(r => (isView(state.currentRuleView, r) ? Object.assign(r, {body: newBody}) : r)),
      };

    // updating filter
    case RulesActions.CHANGE_CURRENT_FILTER:
      return {
        ...state,
        filter: action.payload,
      };

    // deleting rule
    case RulesActions.DELETE_RULE_REQUEST:
      return {
        ...state,
      };
    case RulesActions.DELETE_RULE_SUCCESS:
      return {
        ...state,
        rules: state.rules.filter(r => r.title !== action.payload.title),
      };
    case RulesActions.DELETE_RULE_FAILURE:
      var {rule, message} = action.payload;
      alert(`RULE_DELETION_FAILURE ${message}`);
      return {
        ...state,
      };

    //Updating the interim title for a rule
    case RulesActions.UPDATE_INTERIM_TITLE:
      return {
        ...state,
        rules: state.rules.map(
          r => (isView(state.currentRuleView, r) ? Object.assign(r, {newTitle: action.payload}) : r),
        ),
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
        rules: state.rules.map(r => (isView(state.currentRuleView, r) ? Object.assign(r, {title: newTitle}) : r)),
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
