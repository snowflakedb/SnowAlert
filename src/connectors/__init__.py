import cloudtrail
import okta

CONNECTION_OPTIONS = [
    {'connector': 'cloudtrail', 'options': cloudtrail.CONNECTION_OPTIONS},
    {'connector': 'okta', 'options': okta.CONNECTION_OPTIONS}
]
