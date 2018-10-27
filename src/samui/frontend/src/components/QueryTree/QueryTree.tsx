import {Tree} from 'antd';
import * as React from 'react';
import {connect} from 'react-redux';
import {bindActionCreators, Dispatch} from 'redux';

import {loadSnowAlertRulesIfNeeded} from '../../actions/rules';
import {getSnowAlertRules} from '../../reducers/rules';

import {State, SnowAlertRule, SnowAlertRulesState} from '../../reducers/types';

import './QueryTree.css';

const TreeNode = Tree.TreeNode;

interface OwnProps {}

interface DispatchProps {
  loadSnowAlertRules: typeof loadSnowAlertRulesIfNeeded;
}

interface StateProps {
  rules: SnowAlertRulesState;
}

type QueryTreeProps = OwnProps & DispatchProps & StateProps;

class QueryTree extends React.PureComponent<QueryTreeProps> {
  componentDidMount() {
    this.props.loadSnowAlertRules();
  }

  generateTree = (data: SnowAlertRulesState['rules'], target: SnowAlertRule['target']) => {
    const queryTitles: Array<string> = [];
    const suppressionTitles: Array<string> = [];

    for (let d of data)
      if (d.target === target) {
        if (d.type === 'query') {
          queryTitles.push(d.title);
        }
        if (d.type === 'suppression') {
          suppressionTitles.push(d.title);
        }
      }

    return [
      queryTitles.map(x => <TreeNode key={x} selectable={true} title={x} />),
      suppressionTitles.map(x => <TreeNode key={x} selectable={true} title={x} />),
    ];
  };

  render() {
    var data = this.props.rules.rules;
    return (
      <div>
        <Tree showLine>
          <TreeNode title="Alerts" selectable={false}>
            {this.props.rules.isFetching ? <TreeNode title="Loading..." /> : this.generateTree(data, 'alert')}
          </TreeNode>
        </Tree>

        <Tree showLine>
          <TreeNode title="Violations">
            {this.props.rules.isFetching ? <TreeNode title="Loading..." /> : this.generateTree(data, 'violation')}
          </TreeNode>
        </Tree>
      </div>
    );
  }
}

const mapStateToProps = (state: State) => {
  return {
    rules: getSnowAlertRules(state),
  };
};

const mapDispatchToProps = (dispatch: Dispatch) => {
  return bindActionCreators(
    {
      loadSnowAlertRules: loadSnowAlertRulesIfNeeded,
    },
    dispatch,
  );
};

const ConnectedQueryTree = connect<StateProps, DispatchProps>(
  mapStateToProps,
  mapDispatchToProps,
)(QueryTree);
export default ConnectedQueryTree;
