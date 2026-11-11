def _setting(ctx):
    return [DefaultInfo(), ConstraintSettingInfo(label = ctx.label.raw_target())]

constraint_setting = rule(impl = _setting, attrs = {})

def _value(ctx):
    setting = ctx.attrs.setting[ConstraintSettingInfo]
    value = ConstraintValueInfo(setting = setting, label = ctx.label.raw_target())
    return [
        DefaultInfo(),
        value,
        ConfigurationInfo(constraints = {setting.label: value}, values = {}),
    ]

constraint_value = rule(impl = _value, attrs = {"setting": attrs.dep(providers = [ConstraintSettingInfo])})
