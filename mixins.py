class ContextSwitchMixin(object):
    """Designed for a/b testing ie url?switch=ab
    """
    def get_context_data(self, **kwargs):
        context = super(ContextSwitchMixin, self).get_context_data(**kwargs)
        switch = self.request.GET.get('switch', None)
        if switch:
            context.update({
                'ctx_switch': 'ctx_switch_{}'.format(switch)
            })
        return context
