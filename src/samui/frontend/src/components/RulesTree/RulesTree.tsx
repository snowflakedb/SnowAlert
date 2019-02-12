import {
  // Icon,
  Tree,
  Input,
} from 'antd';
import * as React from 'react';
import * as _ from 'lodash';
import {connect} from 'react-redux';
import {bindActionCreators, Dispatch} from 'redux';

import {loadSnowAlertRules, changeRule, changeFilter} from '../../actions/rules';
import {getRules} from '../../reducers/rules';

import {State, SnowAlertRule, SnowAlertRulesState} from '../../reducers/types';
import {Query} from '../../store/rules';
import {QueryEditor} from '../RuleEditors';

import './RulesTree.css';

const TreeNode = Tree.TreeNode;
const Search = Input.Search;

function allMatchedCaptures(regexp: RegExp, s: string): string[][] {
  var matches: string[][] = [];
  s.replace(regexp, (m, ...args) => {
    matches.push(args.slice(0, args.length - 2));
    return '';
  });
  return matches;
}

interface OwnProps {
  target: SnowAlertRule['target'];
}

interface DispatchProps {
  loadSnowAlertRules: typeof loadSnowAlertRules;
  changeRule: typeof changeRule;
  changeFilter: typeof changeFilter;
}

interface StateProps {
  rules: SnowAlertRulesState;
}

type RulesTreeProps = OwnProps & DispatchProps & StateProps;

class RulesTree extends React.PureComponent<RulesTreeProps> {
  componentDidMount() {
    this.props.loadSnowAlertRules();
    this.props.changeRule('');
  }

  qe = QueryEditor;

  generateTree = (
    queries: ReadonlyArray<Query>,
    rules: SnowAlertRulesState['rules'],
    target: SnowAlertRule['target'],
  ) => {
    const suppressions: Array<SnowAlertRule> = [];
    var filter = this.props.rules.filter;

    function queryFilter(query: Query) {
      let filterTags = _.flatten(allMatchedCaptures(/tag\[([^\]]+)\]/g, filter));
      return (
        filter === '' ||
        query.view_name.includes(filter.toUpperCase()) ||
        query.raw.body.toUpperCase().includes(filter.toUpperCase()) ||
        _.intersection(query.tags, filterTags).length === filterTags.length
      );
    }

    for (let rule of rules)
      if (rule.target === target) {
        if (rule.type === 'SUPPRESSION' && (filter == '' || rule.title.includes(filter.toUpperCase()))) {
          suppressions.push(rule);
        }
      }

    return [
      <TreeNode key="queries" title={`${target} Queries`} selectable={false}>
        {this.props.rules.isFetching ? (
          <TreeNode title="Loading..." />
        ) : (
          queries
            .filter(q => target === q.raw.target)
            .filter(queryFilter)
            .map(r => (
              <TreeNode
                selectable
                key={`${r.view_name}`}
                title={(r.isSaving ? '(saving) ' : r.isEdited ? '* ' : '') + r.title}
              />
            ))
        )}
      </TreeNode>,
      <TreeNode key="suppressions" title={`${target} Suppressions`} selectable={false}>
        {this.props.rules.isFetching ? (
          <TreeNode title="Loading..." />
        ) : (
          suppressions.map(r => (
            <TreeNode
              selectable
              key={`${r.title}_${target}_SUPPRESSION`}
              title={(r.isSaving ? '(saving) ' : r.savedBody === r.body ? '' : '* ') + r.title}
            />
          ))
        )}
      </TreeNode>,
    ];
  };

  render() {
    var {
      target,
      rules: {rules, queries, filter},
    } = this.props;
    return (
      <div>
        <Search
          style={{width: 200}}
          placeholder={`${target} Query Name`}
          value={filter}
          onChange={e => this.props.changeFilter(e.target.value)}
        />
        <Tree showLine defaultExpandAll onSelect={x => this.props.changeRule(x[0] || '')}>
          {this.generateTree(queries, rules, target)}
        </Tree>
      </div>
    );
  }
}

const mapStateToProps = (state: State) => {
  return {
    rules: getRules(state),
  };
};

const mapDispatchToProps = (dispatch: Dispatch) => {
  return bindActionCreators(
    {
      loadSnowAlertRules,
      changeRule,
      changeFilter,
    },
    dispatch,
  );
};

export default connect(
  mapStateToProps,
  mapDispatchToProps,
)(RulesTree);
