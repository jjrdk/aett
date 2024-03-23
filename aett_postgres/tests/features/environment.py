def after_scenario(context, scenario):
    try:
        if context.db:
            context.db.close()
    except:
        pass
    try:
        if context.process:
            context.process.terminate()
    except:
        pass
