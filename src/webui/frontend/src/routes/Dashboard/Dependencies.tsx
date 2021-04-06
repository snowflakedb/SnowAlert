import {Button, Card} from 'antd';
import {LoadingOutlined} from '@ant-design/icons';
import React from 'react';
import {connect} from 'react-redux';
import {bindActionCreators, Dispatch} from 'redux';
import {newRule, renameRule} from '../../actions/rules';
import {RuleDashboard} from '../../components/Dashboard';
import '../../index.css';
import {getRules} from '../../reducers/rules';
import * as stateTypes from '../../reducers/types';
import {Query, Suppression} from '../../store/rules';
import BasicLayout from '../../layouts/BasicLayout';
import {history, navigate} from '../../store/history';

import './Alerts.css';

interface OwnProps {
  path: string;
  selected?: string;
}

interface StateProps {
  rules: stateTypes.SnowAlertRulesState;
}

interface DispatchProps {
  newRule: typeof newRule;
  renameRule: typeof renameRule;
}

type Props = OwnProps & StateProps & DispatchProps;

class Dependencies extends React.PureComponent<Props> {
      


  render() {
    
    return (
      <BasicLayout>
        <Card
          className={'card'}
        
        >
            <div>
              <LoadingOutlined /> Loading Rules...
            </div>
          ) : (
            <div>
         
            </div>
          )
        </Card>
      </BasicLayout>
    );
  }
}

const mapStateToProps = (state: stateTypes.State) => {
  return {
    rules: getRules(state),
  };
};

const mapDispatchToProps = (dispatch: Dispatch) => {
  return bindActionCreators(
    {
      newRule,
      renameRule,
    },
    dispatch,
  );
};

export default connect(mapStateToProps, mapDispatchToProps)(Dependencies);
