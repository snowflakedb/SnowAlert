import {Location} from 'history';
import * as React from 'react';
import {connect} from 'react-redux';
import {push} from 'react-router-redux';
import {bindActionCreators, Dispatch} from 'redux';
import {setViewport} from '../actions/viewport';
import {getLocation} from '../reducers/router';
import * as stateTypes from '../reducers/types';
import {getViewport} from '../reducers/viewport';

type StateProps = {
  viewport: stateTypes.ViewportState;
  location: Location | null;
};

type DispatchProps = {
  push: typeof push;
  setViewport: typeof setViewport;
};

type URLSyncProps = StateProps & DispatchProps;

class URLSync extends React.Component<URLSyncProps> {
  updateStateFromUrl = () => {
    const locationPathname = this.props.location ? encodeURI(this.props.location.pathname) : '/';
    this.props.setViewport(locationPathname);
  };

  updateUrlFromState = () => {
    let expectedUrl;
    const {viewport} = this.props.viewport;

    if (viewport) {
      if (viewport !== '/') {
        expectedUrl = decodeURI(viewport);
        if (!this.props.location || expectedUrl !== this.props.location.pathname) {
          this.props.push(expectedUrl);
        }
      }
    } else {
      throw new Error(`No viewport in state! URL cannot be computed!`);
    }
  };

  componentDidMount() {
    // Before mounting (when the app is starting) we parse the URL and update the state (if the URL is not just '/').
    if (!this.props.location || this.props.location.pathname === '/') {
      // Default values are set by the reducers, we just need to update the URL.
      this.updateUrlFromState();
    } else {
      this.updateStateFromUrl();
    }
  }

  componentDidUpdate(prevProps: URLSyncProps) {
    const {location: prevLocation} = prevProps;

    // On location updates (back/forward by the user), we update the state.
    // Other updates are to the state, for which we update the URL.
    if (prevLocation !== this.props.location) {
      this.updateStateFromUrl();
    } else {
      this.updateUrlFromState();
    }
  }

  render() {
    return false;
  }
}

const mapStateToProps = (state: stateTypes.State) => {
  return {
    viewport: getViewport(state),
    location: getLocation(state),
  };
};

const mapDispatchToProps = (dispatch: Dispatch) => {
  return bindActionCreators(
    {
      setViewport,
      push,
    },
    dispatch,
  );
};

const ConnectedURLSync = connect<StateProps, DispatchProps>(
  mapStateToProps,
  mapDispatchToProps,
)(URLSync);
export default ConnectedURLSync;
