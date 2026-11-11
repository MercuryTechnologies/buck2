def _execution_platforms(ctx):
    platform = ExecutionPlatformInfo(
        label = ctx.label.raw_target(),
        configuration = ConfigurationInfo(constraints = {}, values = {}),
        executor_config = CommandExecutorConfig(
            local_enabled = True,
            remote_enabled = False,
            remote_cache_enabled = False,
        ),
    )
    return [DefaultInfo(), ExecutionPlatformRegistrationInfo(platforms = [platform])]

execution_platforms = rule(attrs = {}, impl = _execution_platforms)

def _platform(ctx):
    return [
        DefaultInfo(),
        PlatformInfo(
            label = str(ctx.label.raw_target()),
            configuration = ConfigurationInfo(constraints = {}, values = {}),
        ),
    ]

platform = rule(attrs = {}, impl = _platform)
