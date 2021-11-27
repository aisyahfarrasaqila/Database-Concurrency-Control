class Transaction:
    def __init__(self, name):
        self.name = name
        self.operations = []

    def addOperation(self, operation):
        self.operations.append(operation)

class Lock:
    def __init__(self, data, transaction):
        self.data = data
        self.transaction = transaction

class LockManager:
    def __init__(self):
        self.locks = []

    def acquireLock(self, lock):
        self.locks.append(lock)

    def releaseLock(self, lock):
        self.locks.remove(lock)

    def isLocked(self, data):
        found = False
        i = 0
        while not found and i < len(self.locks):
            if self.locks[i].data == data:
                found = True
        return found

import sys

directory = sys.argv[1]
file = open(directory, 'r')

schedule = []
transactions = []

for line in file:
    # Read operations in schedule
    name = line.split(' ')[0]
    action = line.split(' ')[1]
    data = line.split(' ')[2].strip()

    # Look for transaction
    found = False
    i = 0
    while not found and i < len(transactions):
        if transactions[i].name == name:
            found = True
        i += 1

    # Add operation to transaction if exists
    if found:
        transactions[i-1].addOperation((action, data))
    # Make transaction object if not
    else:
        transaction = Transaction(name)
        transaction.addOperation((action, data))
        transactions.append(transaction)
    
    schedule.append((name, action, data))

for transaction in transactions:
    print(transaction.name)
    print(transaction.operations)

print("Schedule:\n", schedule)

# Execute schedule
final_schedule = []

lockManager = LockManager()

for operation in schedule:
    name = operation[0]
    data = operation[2]
    # if lock acquisition feasible

