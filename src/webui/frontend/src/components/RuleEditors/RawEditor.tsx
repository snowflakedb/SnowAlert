import {Button} from 'antd';
import {
  LoadingOutlined,
  UploadOutlined,
  RollbackOutlined,
  DeleteOutlined,
  CheckCircleOutlined,
} from '@ant-design/icons';

import * as React from 'react';
import {connect} from 'react-redux';
import {bindActionCreators, Dispatch} from 'redux';
import {getRules} from '../../reducers/rules';
import {updateRuleBody, saveRule, deleteRule} from '../../actions/rules';
import {State, SnowAlertRulesState} from '../../reducers/types';
import sqlFormatter from 'snowsql-formatter';
import './RawEditor.css';
import {Query, Suppression} from '../../store/rules';
import Codemirror from './codemirror_wrapper';

interface OwnProps {
  currentRuleView: string | null;
}

interface DispatchProps {
  updateRuleBody: typeof updateRuleBody;
  saveRule: typeof saveRule;
  deleteRule: typeof deleteRule;
}

interface StateProps {
  rules: SnowAlertRulesState;
}

type RawEditorProps = OwnProps & DispatchProps & StateProps;

class RawEditor extends React.Component<RawEditorProps> {
  state = {formatBoolean: false, revertBoolean: false};

  render(): JSX.Element {
    const {formatBoolean, revertBoolean} = this.state;

    const {currentRuleView, deleteRule, saveRule, updateRuleBody} = this.props;
    const {queries, suppressions} = this.props.rules;
    const rules = [...queries, ...suppressions];
    const rule = rules.find((r) => r.viewName === currentRuleView);

    return (
      <div>
        <Codemirror
          key={rule?.viewName}
          editorInitialValue={rule ? rule.raw.body.toString() : ''}
          editorRule={rule!}
          updateRuleBody={updateRuleBody}
          effectFormat={this.state.formatBoolean}
          effectRevert={this.state.revertBoolean}

        />
        <div className="app"></div>
        <Button
          type="primary"
          disabled={!rule || rule.isSaving || (rule.isSaved && !rule.isEdited)}
          onClick={() => rule && saveRule(rule)}
        >
          {rule && rule.isSaving ? <LoadingOutlined /> : <UploadOutlined />} Apply
        </Button>
        <Button
          type="default"
          disabled={!rule || rule.isSaving || (rule.isSaved && !rule.isEdited)}
          onClick={() => {
            //rule && updateRuleBody(rule.viewName, rule.raw.savedBody)
            this.setState({revertBoolean: !revertBoolean});
          }}
        >
          <RollbackOutlined /> Revert
        </Button>

        <Button type="default" disabled={!rule || rule.isSaving} onClick={() => rule && deleteRule(rule.raw)}>
          <DeleteOutlined /> Delete
        </Button>
        <Button
          type="default"
          disabled={!rule || rule.isSaving}
          onClick={() => {
            //rule && updateRuleBody(rule.viewName, sqlFormatter.format(rule.raw.body));
            this.setState({formatBoolean: !formatBoolean});
            //Upon clicking, triggers a state change that causes the 'useEffect' in codemirror_wrapper to replace contets of the editor with a formatted value.
            //the useEffect in the wrapper is given this state through the 'effectFormat' prop given to the codemirror component
          }}
        >
          <CheckCircleOutlined /> Format
        </Button>
      </div>
    );
  }
}

const mapDispatchToProps = (dispatch: Dispatch) => {
  return bindActionCreators(
    {
      updateRuleBody,
      saveRule,
      deleteRule,
    },
    dispatch,
  );
};

const mapStateToProps = (state: State) => {
  return {
    rules: getRules(state),
  };
};

export default connect(mapStateToProps, mapDispatchToProps)(RawEditor);
