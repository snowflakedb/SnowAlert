import * as React from 'react';
import {RouterState} from 'react-router-redux';

export interface AuthDetails {
  readonly isAuthenticated: boolean;
  readonly token: string | null;
  readonly email: string | null;
  readonly role: string | null;
  readonly organizationId: number | null;
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
  readonly notifications: ReadonlyArray<Notification>;
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
  readonly target: 'alert' | 'violation';
  readonly type: 'query' | 'suppression';
  readonly title: string;
  readonly body: string;
}

export interface SnowAlertRulesState {
  readonly errorMessage: null;
  readonly isFetching: boolean;
  readonly rules: ReadonlyArray<SnowAlertRule>;
}

export interface State {
  readonly auth: AuthState;
  readonly notifications: NotificationsState;
  readonly organization: OrganizationState;
  readonly router: RouterState;
  readonly viewport: ViewportState;
  readonly rules: SnowAlertRulesState;
  // readonly loaded_rule_name: string;
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
