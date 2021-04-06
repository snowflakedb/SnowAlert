import {composeWithDevTools} from 'redux-devtools-extension';
import thunk from 'redux-thunk';
import {createStore, applyMiddleware} from 'redux';
import {cmreducer} from "../reducers/cmreducer";
const cmstore = createStore(cmreducer, composeWithDevTools(applyMiddleware(thunk)));

export default cmstore;
