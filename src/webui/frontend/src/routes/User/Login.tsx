import {Button, Form, Icon, Input} from 'antd';
import * as React from 'react';
import {connect} from 'react-redux';
import {bindActionCreators, Dispatch} from 'redux';
import {oauthLogin, oauthRedirect} from '../../actions/auth';
import * as stateTypes from '../../reducers/types';
import './Login.css';

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

  login = (e: React.FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    this.props.form.validateFields((err: string, values: FormProps) => {
      if (!err) {
        localStorage.setItem('account', values.account);
        this.props.oauthRedirect(values.account, window.location.href);
      } else {
        this.setState({
          errorMessage: err,
        });
      }
    });
  };

  render() {
    const {getFieldDecorator} = this.props.form;
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
          <img src="/icons/favicon.ico" style={{height: 50}} /> SnowAlert
        </h1>
        {code ? (
          <div className={'main'}>
            <h2>Acquiring Access Token</h2>
            <h5>Buckle up your seatbelt, Dorothy</h5>
            <Icon type="loading" style={{marginLeft: 250, marginTop: 50}} />
          </div>
        ) : (
          <div className={'main'}>
            <h2>Sign in to your account</h2>
            <h5>Enter your account's Snowflake URL</h5>
            <Form className={'login'} onSubmit={this.login}>
              <Form.Item>
                {getFieldDecorator('account', {
                  initialValue: account || '',
                  rules: [
                    {
                      required: true,
                      message: 'Enter your account name',
                    },
                  ],
                })(
                  <Input
                    prefix={<Icon className={'prefix-icon'} type={'api'} />}
                    placeholder={'your-account-url'}
                    addonAfter={'.snowflakecomputing.com'}
                  />,
                )}
              </Form.Item>
              <Form.Item style={{marginBottom: '12px'}}>
                <Button type={'primary'} size={'large'} htmlType={'submit'} className={'form-button'}>
                  Continue &rarr;
                </Button>
              </Form.Item>
            </Form>
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

const ConnectedLogin = connect<StateProps, DispatchProps>(
  mapStateToProps,
  mapDispatchToProps,
)(Form.create()(LoginForm));
export default ConnectedLogin;
