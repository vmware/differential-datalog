#!/usr/bin/python

import random
from sys import (
    argv,
    exit,
)


def main(args):
    """Generate a transaction trace for ip_discovery."""
    # The seed to use for initializing the random number generator.
    seed = int(args[0])
    # Number of transactions to generate.
    count = int(args[1])
    time = 0

    random.seed(a=seed)

    for _ in range(count):
        # Whether to generate a 'LogicalSwitchPort' transaction or not.
        lsp = random.randrange(10) == 0
        if lsp:
            switch = random.randrange(20)
            port = random.randrange(255)
            tofu = random.choice(("true", "false"))
            print("start;")
            print(f"insert LogicalSwitchPort({port}, {switch}, {tofu});")
            print("commit;")
        else:
            count = random.randrange(1, 10)
            print("start;")

            for _ in range(count):
                port = random.randrange(255)
                addr = "192.168.{}.{}".format(random.randrange(255), random.randrange(255))
                print(f"""insert SnoopedAddress({port}, "{addr}", {time});""")
                time += 1

            print("commit;")
    return 0


if __name__ == "__main__":
    exit(main(argv[1:]))
