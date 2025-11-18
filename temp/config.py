def _validate_db_type(config: ConfigDict, errors: list[str], logger: logging.Logger | None) -> None:
    """Валидирует тип базы данных."""
    db_type = config.get('DB_TYPE', '')
    if not isinstance(db_type, str):
        msg = 'DB_TYPE должен быть строкой'
        errors.append(msg)
        if logger:
            logger.error(msg)
        return

    if db_type.lower() in VALID_DB_TYPES:
        if logger:
            logger.debug('DB_TYPE валиден: %s', db_type)
        return

    msg = f"Некорректный DB_TYPE: '{db_type}'. Допустимые значения: {', '.join(VALID_DB_TYPES)}"
    errors.append(msg)
    if logger:
        logger.error(msg)
