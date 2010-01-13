def get_common_email_provider(email):
    """ Returns a (name, login_url) tuple of a common webmail host from email,
    or None. """
    try:
        (name, domain) = email.split('@')
    except ValueError:
        return None
    providers = { 'gmail.com': {
            'name':'GMail',
            'url':'http://www.google.com/mail'
        }, 'hotmail.com': {
            'name':'Hotmail',
            'url':'http://www.hotmail.com'
        }, 'yahoo.com': {
            'name':'Yahoo Mail',
            'url':'http://mail.yahoo.com'
        }, 'aol.com': {
            'name':'AOL Mail',
            'url':'http://mail.aol.com'
        }
    }
    return providers.get(domain.strip().lower())
