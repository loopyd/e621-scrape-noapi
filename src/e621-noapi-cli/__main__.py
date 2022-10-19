from .cli import (
    main,
    parse_args
)
import sys

main(parse_args(sys.argv))