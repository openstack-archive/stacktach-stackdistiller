# Event Definitions file format

The event definitions file is in YAML format. It consists of a list of event
definitions, which are mappings. Order is significant, the list of definitions
is scanned in *reverse* order (last definition in the file to the first),
to find a definition which matches the notification's event_type.  That
definition will be used to generate the Event. The reverse ordering is done
because it is common to want to have a more general wildcarded definition
(such as `compute.instance.*` ) with a set of traits common to all of those
events, with a few more specific event definitions (like
`compute.instance.exists`) afterward that have all of the above traits, plus
a few more. This lets you put the general definition first, followed by the
specific ones, and use YAML mapping [merge key syntax](http://yaml.org/type/merge.html)
to avoid copying all of the trait definitions.

## Event Definitions

Each event definition is a mapping with two keys (both required):

    event_type
        This is a list (or a string, which will be taken as a 1 element list)
        of event_types this definition will handle. These can be
        wildcarded with unix shell glob syntax. An exclusion listing
        (starting with a '!') will exclude any types listed from matching.
        If ONLY exclusions are listed, the definition will match anything
        not matching the exclusions.
    traits
        This is a mapping, the keys are the trait names, and the values are
        trait definitions.

## Trait Definitions

Each trait definition is a mapping with the following keys:

    type
        (optional) The data type for this trait. (as a string). Valid
        options are: text, int, float, and datetime.
        defaults to text if not specified.
    fields
        A path specification for the field(s) in the notification you wish
        to extract for this trait. Specifications can be written to match
        multiple possible fields, the value for the trait will be derived
        from the matching fields that exist and have a non-null values in
        the notification. By default the value will be the first such field.
        (plugins can alter that, if they wish). This is normally a string,
        but, for convenience, it can be specified as a list of
        specifications, which will match the fields for all of them. (See
        "Field Path Specifications" below for more info on this syntax.)
    plugin
        (optional) This is a mapping (For convenience, this value can also
        be specified as a string, which is interpreted as the name of a
        plugin to be loaded with no parameters) with the following keys

        name
            (string) name of a plugin to load

        parameters
            (optional) Mapping of keyword arguments to pass to the plugin on
            initialization. (See documentation on each plugin to see what
            arguments it accepts.)

## Field Path Specifications

The path specifications define which fields in the JSON notification
body are extracted to provide the value for a given trait.  The paths
can be specified with a dot syntax (e.g. `payload.host`). Square
bracket syntax (e.g. `payload[host]`) is also supported. In either
case, if the key for the field you are looking for contains special
characters, like '.', it will need to be quoted (with double or single
quotes) like so:

    payload.image_meta.'org.openstack__1__architecture'

The syntax used for the field specification is a variant of JSONPath,
and is fairly flexible. (see: https://github.com/kennknowles/python-jsonpath-rw for more info)

## Example Definitions file

    ---
    - event_type: compute.instance.*
      traits: &instance_traits
        user_id:
          fields: payload.user_id
        instance_id:
          fields: payload.instance_id
        host:
          fields: publisher_id
          plugin:
            name: split
            parameters:
              segment: 1
              max_split: 1
        service_name:
          fields: publisher_id
          plugin: split
        instance_type_id:
          type: int
          fields: payload.instance_type_id
        os_architecture:
          fields: payload.image_meta.'org.openstack__1__architecture'
        launched_at:
          type: datetime
          fields: payload.launched_at
        deleted_at:
          type: datetime
          fields: payload.deleted_at
    - event_type:
        - compute.instance.exists
        - compute.instance.update
      traits:
        <<: *instance_traits
        audit_period_beginning:
          type: datetime
          fields: payload.audit_period_beginning
        audit_period_ending:
          type: datetime
          fields: payload.audit_period_ending

## Trait plugins

Trait plugins can be used to do simple programmatic conversions on the value in
a notification field, like splitting a string, lowercasing a value, converting
a screwball date into ISO format, or the like.

Plugins are initialized with the parameters from the trait definition, if any,
which can customize their behavior for a given trait.
They are called with a list of all matching fields from the notification,
so they can derive a value from multiple fields.

The plugin will be called even if there is no fields found matching the field
path(s), this lets a plugin set a default value, if needed.
A plugin can also reject a value by returning `None`, which will cause the trait not to be
added.

If the plugin returns anything other than `None`, the trait's value
will be set from whatever the plugin returned (coerced to the appropriate type
for the trait).
