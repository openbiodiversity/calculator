get_window_url_params = """
    function() {
        const params = new URLSearchParams(window.location.search);
        const url_params = Object.fromEntries(params);
        console.log('url_params', url_params)
        return url_params;
        }
    """