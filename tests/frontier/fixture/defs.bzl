# Prelude-free rules for the frontier fixture: every action runs locally with /bin/sh, and
# nothing reaches a remote backend or a system compiler.

load(":lib.bzl", "out_name")

def _write_txt(ctx):
    out = ctx.actions.write(out_name(ctx.label.name, "txt"), ctx.attrs.content, has_content_based_path = False)
    return [DefaultInfo(default_output = out)]

write_txt = rule(impl = _write_txt, attrs = {
    "content": attrs.string(),
    "deps": attrs.list(attrs.dep(), default = []),
})

def _run_sh(ctx):
    out = ctx.actions.declare_output(out_name(ctx.label.name, "out"), has_content_based_path = False)
    ctx.actions.run(
        cmd_args("/bin/sh", ctx.attrs.tool, out.as_output(), ctx.attrs.mode, ctx.attrs.srcs),
        category = "run_sh",
        local_only = True,
    )
    return [DefaultInfo(default_output = out)]

run_sh = rule(impl = _run_sh, attrs = {
    "mode": attrs.string(),
    "srcs": attrs.list(attrs.source()),
    "tool": attrs.source(),
})

def _copy(ctx):
    out = ctx.actions.copy_file(out_name(ctx.label.name, "copy"), ctx.attrs.src, has_content_based_path = False)
    return [DefaultInfo(default_output = out)]

copy = rule(impl = _copy, attrs = {"src": attrs.source()})

def _tree(ctx):
    out = ctx.actions.symlinked_dir(ctx.label.name, {k: v for k, v in ctx.attrs.srcs.items()}, has_content_based_path = False)
    return [DefaultInfo(default_output = out)]

tree = rule(impl = _tree, attrs = {"srcs": attrs.dict(attrs.string(), attrs.source())})

def _dyn(ctx):
    out = ctx.actions.declare_output("dyn.txt", has_content_based_path = False)

    def dynamic(ctx, out):
        inner = ctx.actions.write("dyn_inner.txt", "dynamic", has_content_based_path = False)
        ctx.actions.copy_file(out.as_output(), inner, has_content_based_path = False)

    ctx.actions.dynamic_output(dynamic = [], inputs = [], outputs = [out.as_output()], f = lambda ctx, _dyn, outputs: dynamic(ctx, outputs[out]))
    return [DefaultInfo(default_output = out)]

dyn = rule(impl = _dyn, attrs = {})

def _many(ctx):
    # Enough outputs to use the large output map, whose order is insertion order.
    outs = [ctx.actions.declare_output("many/{}.txt".format(i), has_content_based_path = False) for i in range(40)]
    ctx.actions.run(
        cmd_args("/bin/sh", "-c", "for f in \"$@\"; do printf '%s\\n' \"$f\" > \"$f\"; done", "sh", [o.as_output() for o in outs]),
        category = "many",
        local_only = True,
    )
    return [DefaultInfo(default_outputs = outs)]

many = rule(impl = _many, attrs = {})
