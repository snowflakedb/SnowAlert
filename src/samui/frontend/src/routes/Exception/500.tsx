import * as React from 'react';
import {Exception} from '../../components/Exception';
import * as exceptionTypes from '../../constants/exceptionTypes';

export default () => <Exception type={exceptionTypes.INTERNAL_SERVER_ERROR} style={{minHeight: 500, height: '80%'}} />;
