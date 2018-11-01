import * as React from 'react';
import {connect} from 'react-redux';
import {bindActionCreators, Dispatch} from 'redux';
import {loginSuccess} from '../actions/auth';
import {setViewport} from '../actions/viewport';
import * as api from '../api';
import * as routes from '../constants/routes';
import {getAuthDetails} from '../reducers/auth';
import {AuthDetails, State} from '../reducers/types';

interface OwnProps {
  component: any;
  roles: ReadonlyArray<string>;
}

interface DispatchProps {
  loginSuccess: typeof loginSuccess;
  setViewport: typeof setViewport;
}

interface StateProps {
  auth: AuthDetails;
}

type AuthorizedRouteProps = OwnProps & DispatchProps & StateProps;

class AuthorizedRoute extends React.Component<AuthorizedRouteProps> {
  componentDidMount() {
    this.checkAuth();

    // Redirect to login if needed.
    const {auth, roles} = this.props;
    if (!auth.isAuthenticated || !auth.role || (roles && roles.length > 0 && roles.indexOf(auth.role) === -1)) {
      // authentication-override
      // this.props.setViewport(routes.LOGIN);
    }
  }

  componentDidUpdate() {
    this.checkAuth();
  }

  checkAuth(props: AuthorizedRouteProps = this.props) {
    if (!props.auth.isAuthenticated) {
      const token = localStorage.getItem('token');
      if (token) {
        api.validateToken(token).then(() => {
          this.props.loginSuccess(token);
        });
      }
    }
  }

  render() {
    const {component: Component} = this.props;
    return <Component />;
  }
}

const mapStateToProps = (state: State) => {
  return {
    auth: getAuthDetails(state),
  };
};

const mapDispatchToProps = (dispatch: Dispatch) => {
  return bindActionCreators(
    {
      loginSuccess,
      setViewport,
    },
    dispatch,
  );
};

const ConnectedAuthorizedRoute = connect<StateProps, DispatchProps>(
  mapStateToProps,
  mapDispatchToProps,
)(AuthorizedRoute);
export default ConnectedAuthorizedRoute;
