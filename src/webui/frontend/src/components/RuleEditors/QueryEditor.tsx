import {Button, Col, Icon, Input, Switch, Tag} from 'antd';
import * as React from 'react';
import {connect} from 'react-redux';
import {bindActionCreators, Dispatch} from 'redux';
import * as _ from 'lodash';

import {getRules} from '../../reducers/rules';
import {Query, Suppression} from '../../store/rules';
import {updateRuleBody, updateRule, saveRule, deleteRule, addTagFilter, removeTagFilter} from '../../actions/rules';
import EditableTagGroup from '../EditableTagGroup';

import {State, RuleTarget, SnowAlertRulesState} from '../../reducers/types';

import './QueryEditor.css';

const {CheckableTag} = Tag;

interface TagToggleProps {
  onCheckedOn: () => any;
  onCheckedOff: () => any;
  defaultChecked: boolean;
  size: number;
}

interface TagToggleState {
  checked: boolean;
}

class TagToggle extends React.Component<TagToggleProps, TagToggleState> {
  state = {checked: this.props.defaultChecked};

  handleChange = (checked: boolean) => {
    this.setState({checked});
    if (checked) {
      this.props.onCheckedOn();
    } else {
      this.props.onCheckedOff();
    }
  };

  render() {
    return (
      <span style={{zoom: 1 + Math.log(this.props.size)}}>
        <CheckableTag checked={this.state.checked} onChange={this.handleChange}>
          {this.props.children}
        </CheckableTag>
      </span>
    );
  }
}

interface StringFieldsDefinition {
  title: string;
  type: 'string' | 'text';
  getValue(r: any): string;
  setValue(q: Query | Suppression, v: string): Query | Suppression;
}

export interface QueryEditorColumn {
  span: number;
  fields: Array<StringFieldsDefinition | BoolFieldDefinition | TagGroupFieldDefinition>;
}

interface BoolFieldDefinition {
  title: string;
  type: 'boolean';
  getValue(r: any): boolean;
  setValue(q: Query | Suppression, v: boolean): Query | Suppression;
}

interface TagGroupFieldDefinition {
  title: string;
  type: 'tagGroup';
  getValue(r: any): string;
  setValue(q: Query | Suppression, v: string): Query | Suppression;
}

interface OwnProps {
  cols: QueryEditorColumn[];
  target: RuleTarget;
}

interface DispatchProps {
  updateRuleBody: typeof updateRuleBody;
  updateRule: typeof updateRule;
  saveRule: typeof saveRule;
  deleteRule: typeof deleteRule;
  addTagFilter: typeof addTagFilter;
  removeTagFilter: typeof removeTagFilter;
}

interface StateProps {
  rules: SnowAlertRulesState;
}

type QueryEditorProps = OwnProps & DispatchProps & StateProps;

class QueryEditor extends React.PureComponent<QueryEditorProps> {
  getTagArray(qs: ReadonlyArray<Query | Suppression>) {
    const tagCounts: {
      [tagName: string]: number;
    } = _.flatMap(Array.from(qs), q => q.tags || []).reduce(
      (ts, t) => Object.assign(ts, {[t]: ts[t] ? ts[t] + 1 : 1}),
      {},
    );

    return Object.entries(tagCounts).map(([tag, count]) => (
      <TagToggle
        key={tag}
        size={count}
        onCheckedOn={() => this.props.addTagFilter(tag)}
        onCheckedOff={() => this.props.removeTagFilter(tag)}
        defaultChecked={!!this.props.rules.filter.match(new RegExp(`\b${tag}\b`))}
      >
        {tag}
      </TagToggle>
    ));
  }

  render() {
    const {updateRule, updateRuleBody, cols, saveRule} = this.props;
    const {currentRuleView, queries, suppressions} = this.props.rules;
    const rules = [...queries, ...suppressions].filter(q => q.target === this.props.target);
    const q = rules.find(q => q.viewName === currentRuleView);

    if (!(currentRuleView && q && q.isParsed)) {
      return (
        <Col span={16}>
          <h3>
            {rules.filter(r => r.isParsed).length} parsed / {rules.length} loaded from Snowflake
          </h3>
          {this.getTagArray(rules)}
        </Col>
      );
    }

    return (
      <div>
        {cols.map((col, i) => (
          <Col key={`col-${i}`} span={col.span}>
            {col.fields.map((field, i) =>
              field.type === 'string' ? (
                <div key={`col-${i}`}>
                  <h3>{field.title}</h3>
                  <Input.TextArea
                    disabled={q.isSaving}
                    spellCheck={false}
                    value={field.getValue(q)}
                    onChange={e => updateRule(currentRuleView, field.setValue(q, e.target.value))}
                  />
                </div>
              ) : field.type === 'text' ? (
                <div key={`col-${i}`}>
                  <h3>{field.title}</h3>
                  <Input.TextArea
                    disabled={q.isSaving}
                    spellCheck={false}
                    value={field.getValue(q)}
                    onChange={e => updateRule(currentRuleView, field.setValue(q, e.target.value))}
                  />
                </div>
              ) : field.type === 'boolean' ? (
                <div key={`col-${i}`}>
                  <h3>{field.title}</h3>
                  <Switch
                    disabled={q.isSaving}
                    defaultChecked={field.getValue(q)}
                    onChange={e => updateRule(currentRuleView, field.setValue(q, e))}
                  >
                    {field.title}
                  </Switch>
                </div>
              ) : field.type === 'tagGroup' ? (
                <div key={`col-${i}`}>
                  <h3>{field.title}</h3>
                  <EditableTagGroup
                    disabled={q.isSaving}
                    tags={field.getValue(q)}
                    onChange={e => updateRule(currentRuleView, field.setValue(q, e))}
                  >
                    {field.title}
                  </EditableTagGroup>
                </div>
              ) : null,
            )}
          </Col>
        ))}
        <Col span={24}>
          <Button
            type="primary"
            disabled={q.isSaving || (!q.isEdited && q.isSaved)}
            onClick={() => q && saveRule(q.raw)}
          >
            {q && q.isSaving ? <Icon type="loading" theme="outlined" /> : <Icon type="upload" />} Apply
          </Button>
          <Button
            type="default"
            disabled={q.isSaving || (!q.isEdited && q.isSaved)}
            onClick={() => q && updateRuleBody(q.raw.savedBody)}
          >
            <Icon type="rollback" theme="outlined" /> Revert
          </Button>
          <Button type="default" disabled={q.isSaving || !q.isSaved} onClick={() => q && this.props.deleteRule(q.raw)}>
            <Icon type="delete" theme="outlined" /> Delete
          </Button>
        </Col>
      </div>
    );
  }
}

const mapDispatchToProps = (dispatch: Dispatch) => {
  return bindActionCreators(
    {
      updateRuleBody,
      updateRule,
      saveRule,
      deleteRule,
      addTagFilter,
      removeTagFilter,
    },
    dispatch,
  );
};

const mapStateToProps = (state: State) => {
  return {
    rules: getRules(state),
  };
};

export default Object.assign(
  connect(
    mapStateToProps,
    mapDispatchToProps,
  )(QueryEditor),
);
