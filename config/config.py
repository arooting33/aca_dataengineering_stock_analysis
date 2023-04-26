from configparser import ConfigParser


def config_db(filename: str = 'database.ini', section: str = 'postgresql') -> dict:
    """
    for storing sensitive information we use config files, for not having our password in code
    .ini files
    :param filename:
    :param section:
    :return:
    """
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)

    # get section, default to postgresql
    # creating a dictionary, for convenient access to values
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return db


if __name__ == '__main__':
    config_db()