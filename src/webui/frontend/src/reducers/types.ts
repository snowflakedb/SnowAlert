import * as React from 'react';
import {RouterState} from 'react-router-redux';
import {Connector} from '../store/data';
import {Policy, Query, Suppression} from '../store/rules';

export interface AuthDetails {
  readonly token: string | null;
  readonly email: string | null;
  readonly role: string | null;
  readonly avatar: string | undefined;
}

export interface AuthState extends AuthDetails {
  readonly isFetching: boolean;
  readonly errorMessage: string | null;
}

export interface AuthStatus {
  readonly isFetching: boolean;
  readonly isAuthenticated: boolean;
  readonly errorMessage: string | null;
}

export interface Notification {
  readonly id: number;
  readonly title: string;
  readonly description?: string;
  readonly timestamp: string;
}

export interface NotificationsState {
  readonly errorMessage: string | null;
  readonly isFetching: boolean;
}

export interface Organization {
  readonly title: string;
}

export interface OrganizationState {
  readonly errorMessage: string | null;
  readonly isFetching: boolean;
  readonly details: Organization | null;
}

export interface ViewportState {
  readonly viewport: string;
}

export interface SnowAlertRule {
  readonly target: 'ALERT' | 'VIOLATION' | 'POLICY';
  readonly type: 'QUERY' | 'SUPPRESSION' | 'DEFINITION';
  readonly passing?: boolean;
  readonly isEditing?: boolean;
  readonly results?: Array<{TITLE: string; PASSING?: boolean}>;
  readonly title: string;
  readonly body: string;
  readonly savedBody: string;
  readonly isSaving: boolean;
  readonly newTitle: string | null;
}

export type RuleTarget = SnowAlertRule['target'];
export type RuleType = SnowAlertRule['type'];

export interface SnowAlertRulesState {
  readonly errorMessage: null;
  readonly isFetching: boolean;
  readonly policies: ReadonlyArray<Policy>;
  readonly queries: ReadonlyArray<Query>;
  readonly suppressions: ReadonlyArray<Suppression>;
  readonly currentRuleView: string | null;
  readonly filter: string;
}

export interface ConnectorPayload {
  connector: string;
  options: any;
  finalize: boolean;
  docstring: string;
  secret: boolean;
  mask_on_screen: boolean;
  title: string;
  prompt: string;
  placeholder: string;
}

export interface FlowPayload {
  name: string;
  connector: string;
}

export interface SAData {
  connectors: ReadonlyArray<ConnectorPayload>;
  flows: ReadonlyArray<FlowPayload>;
}

export type ConnectionStage = 'start' | 'creating' | 'created' | 'finalizing' | 'finalized' | 'testing' | 'tested';

export interface SADataState {
  readonly isFetching: boolean;
  readonly selected: string | null;
  readonly errorMessage: string | null;
  readonly connectionMessage: string | null;
  readonly connectors: ReadonlyArray<Connector>;
  readonly connectionStage: ConnectionStage;
}

export interface State {
  readonly auth: AuthState;
  readonly router: RouterState;
  readonly viewport: ViewportState;
  readonly rules: SnowAlertRulesState;
  readonly data: SADataState;
}

export interface RouterData {
  readonly [key: string]: {
    readonly name: string;
    readonly roles: ReadonlyArray<string>;
    component?: React.ComponentType<React.ComponentType<any>>;
  };
}

export interface MenuItem {
  readonly name: string;
  readonly path: string;
  readonly target?: string;
  readonly icon?: string;
  readonly hideInMenu?: boolean;
  readonly key?: string;
  readonly roles?: string[];
  readonly children?: ReadonlyArray<MenuItem>;
}

export type MenuData = ReadonlyArray<MenuItem>;
