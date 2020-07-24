import {Button, Collapse, Form, Input} from 'antd';
import {ClusterOutlined, DatabaseOutlined, LoadingOutlined, ApiOutlined, TeamOutlined, SettingOutlined} from '@ant-design/icons';
import * as React from 'react';
import {connect} from 'react-redux';
import {bindActionCreators, Dispatch} from 'redux';
import {oauthLogin, oauthRedirect} from '../../actions/auth';
import * as stateTypes from '../../reducers/types';
import './Login.css';
import {Location} from '@reach/router';

interface OwnProps {
  errorMessage: string;
  isFetching: boolean;
  form: any;
}

interface DispatchProps {
  oauthLogin: typeof oauthLogin;
  oauthRedirect: typeof oauthRedirect;
}

interface StateProps {}

interface State {
  errorMessage: string;
}

type LoginFormProps = OwnProps & DispatchProps & StateProps;

interface FormProps {
  account: string;
}

class LoginForm extends React.Component<LoginFormProps, State> {
  constructor(props: LoginFormProps) {
    super(props);
    this.state = {
      errorMessage: props.errorMessage,
    };
  }

  login = (values: any) => {
    console.log(values)
    localStorage.setItem('account', values.account);
    localStorage.setItem('role', values.role || '');
    localStorage.setItem('database', values.database || '');
    localStorage.setItem('warehouse', values.warehouse || '');
    this.props.oauthRedirect(values.account, values.role, values.database, values.warehouse, window.location.href);
  };

  render() {
    const m = window.location.search.match(/\?code=([0-9A-F]+)/);
    const code = m ? m[1] : null;

    const account = JSON.parse(localStorage.getItem('auth') || '{}').account || localStorage.getItem('account') || '';

    if (code && account) {
      const redirectUri = window.location.origin + window.location.pathname;
      this.props.oauthLogin(account, code, redirectUri);
    }

    return (
      <div className={'login'}>
        <h1>
          <img src="/icons/favicon.ico" style={{height: 50}} alt="snowflake alert sign" /> SnowAlert
        </h1>
        {code ? (
          <div className={'main'}>
            <h2>Acquiring Access Token</h2>
            <h5>Buckle up your seatbelt, Dorothy</h5>
            <LoadingOutlined style={{marginLeft: 190, marginTop: 50}} />
          </div>
        ) : (
          <div className={'main'}>
            <h2>Sign in to your account</h2>
            <h5>Enter your account's Snowflake URL</h5>
            <Location>
              {({navigate}) => (
                <Form className={'login'} onFinish={vs => this.login(vs)}>
                  <Form.Item name="account" rules={[{ required: true }]}>
                    <Input
                      prefix={<ApiOutlined className={'prefix-icon'} />}
                      placeholder={'your-account-url'}
                      addonAfter={'.snowflakecomputing.com'}
                    />
                  </Form.Item>
                  <Collapse>
                    <Collapse.Panel header="Advanced" key="1" extra={<SettingOutlined />}>
                      <Form.Item name="role">
                        <Input
                          prefix={<TeamOutlined className={'prefix-icon'} />}
                          placeholder={'snowalert_reader_rl'}
                          addonBefore={'ROLE'}
                        />
                      </Form.Item>
                      <Form.Item name="database">
                        <Input
                          prefix={<DatabaseOutlined className={'prefix-icon'} />}
                          placeholder={'snowalert_db'}
                          addonBefore={'DATABASE'}
                        />
                      </Form.Item>
                      <Form.Item name="warehouse">
                        <Input
                          prefix={<ClusterOutlined className={'prefix-icon'} />}
                          placeholder={'snowalert_wh'}
                          addonBefore={'WAREHOUSE'}
                        />
                      </Form.Item>
                    </Collapse.Panel>
                  </Collapse>
                  <Form.Item style={{marginBottom: '12px'}}>
                    <Button type={'primary'} size={'large'} htmlType={'submit'} className={'form-button'}>
                      Continue &rarr;
                    </Button>
                  </Form.Item>
                </Form>
              )}
            </Location>
          </div>
        )}
      </div>
    );
  }
}

const mapStateToProps = (state: stateTypes.State) => {
  return {};
};

const mapDispatchToProps = (dispatch: Dispatch) => {
  return bindActionCreators(
    {
      oauthLogin,
      oauthRedirect,
    },
    dispatch,
  );
};

export default connect(mapStateToProps, mapDispatchToProps)(LoginForm);
