import sys

class Lock:
    def __init__(self, data):
        self.data = data
        self.transactions = []

    def enqueue(self, transaction):
        self.transactions.append(transaction)

    def dequeue(self):
        self.transactions.pop(0)

    def isHeldByTransaction(self, transaction):
        return True if self.transactions[0] == transaction else False

    def isWaitedByTransaction(self, transaction):
        found = False
        i = 1
        while not found and i < len(self.transactions):
            if self.transactions[i].transaction == transaction:
                found = True
            i += 1
        return found

class Transaction:
    def __init__(self, transaction):
        self.transaction = transaction
        self.waiting = False

    def setWaiting(self, waiting):
        self.waiting = waiting

    def isWaiting(self):
        return self.waiting

class LockManager:
    def __init__(self):
        self.locks = []
        self.transactions = []

    def findLock(self, data):
        i = 0
        while i < len(self.locks):
            if self.locks[i].data == data:
                return i
            i += 1
        return -1

    def acquireLock(self, transaction, data):
        index = self.findLock(data)
        if index != -1:
            self.locks[index].enqueue(transaction)
        else:
            lock = Lock(data)
            lock.enqueue(transaction)
            self.locks.append(lock)

    def releaseLock(self, transaction, data):
        index = self.findLock(data)
        if index != -1:
            if self.locks[index].transactions[0] != transaction:
                raise Exception('Transaction does not hold the lock')
            else:
                self.locks[index].dequeue()

    def releaseAllLocks(self, transaction):
        for lock in self.locks:
            if lock.isHeldByTransaction(transaction):
                lock.dequeue()

    # List of locks held by a transaction (head of the queue)
    def transactionLocks(self, transaction):
        locks = []
        for lock in self.locks:
            if lock.transactions[0] == transaction:
                locks.append(lock.data)
        return locks

    # True if transaction holds lock
    def isHeld(self, transaction, data):
        index = self.findLock(data)
        if index != -1:
            status = self.locks[index].isHeldByTransaction(transaction)
            return status
        else:
            return False

    def findTransaction(self, transaction):
        i = 0
        while i < len(self.transactions):
            if self.transactions[i].transaction == transaction:
                return i
            i += 1
        return -1

    def addTransaction(self, transaction):
        index = self.findTransaction(transaction)
        if index == -1:
            new_transaction = Transaction(transaction)
            self.transactions.append(new_transaction)

    def setTransactionWaiting(self, transaction, waiting):
        index = self.findTransaction(transaction)
        if index != -1:
            self.transactions[index].setWaiting(waiting)

    def isTransactionWaiting(self, transaction):
        index = self.findTransaction(transaction)
        if index != -1:
            return self.transactions[index].isWaiting()

    def isDeadlock(self, transaction, data):
        index = self.findLock(data)
        if index != -1:
            for item in self.locks[index].transactions:
                locks = self.transactionLocks(item)
                for lock in locks:
                    if any(transaction_lock == lock for transaction_lock in self.transactionLocks(transaction)):
                        return True
        return False

# lockManager = LockManager()
# lockManager.addTransaction(('T2'))
# lockManager.addTransaction('T1')
# lockManager.acquireLock('T2', 'B')


# Read schedule from file
directory = sys.argv[1]
file = open(directory, 'r')

# Make schedule
lockManager = LockManager()

schedule = []
final_schedule = []
waiting_queue = []
for line in file:
    # Read operations in schedule
    transaction = line.split(' ')[0]
    action = line.split(' ')[1]
    data = line.split(' ')[2].strip()
    
    schedule.append((transaction, action, data))
    lockManager.addTransaction(transaction)

# print(schedule)

# Execute schedule
i = 0
while len(schedule) > 0 or len(waiting_queue) > 0:
    
    # Execute waiting queue if possible
    if len(waiting_queue) != 0:
        print('-- EXECUTING WAITING QUEUE --')
        i_queue = 0
        while i_queue < len(waiting_queue):
            operation_queue = waiting_queue[i_queue]
            transaction_queue = operation_queue[0]
            action_queue = operation_queue[1]
            data_queue = operation_queue[2]

            # Check for locks if action is not commit
            if action_queue != 'COMMIT':
                held_queue = lockManager.isHeld(transaction_queue, data_queue)
                # Add operation to final schedule if lock is held
                if held_queue:
                    print(operation_queue)
                    print("Transaction in queue has lock, added to final schedule")
                    final_schedule.append(operation_queue)
                    waiting_queue.pop(i_queue)
                    i_queue -= 1
                # Skip queue if lock is not held
                else:
                    print(operation_queue)
                    print("Still waiting, skip queue\n")
                    break
            else:
                # Release all locks when commiting
                print(operation_queue)
                print("Transaction releases locks\n")
                lockManager.releaseAllLocks(transaction_queue)
                final_schedule.append(operation_queue)
                waiting_queue.pop(i_queue)
                i_queue -= 1

            i_queue += 1

    if len(schedule) != 0:
        print('-- EXECUTING INPUT SCHEDULE --')
        operation = schedule[i]
        transaction = operation[0]
        action = operation[1]
        data = operation[2]

        if not lockManager.isTransactionWaiting(transaction):
            # print(lockManager.isTransactionWaiting(transaction))
            if action != 'COMMIT':
                held = lockManager.isHeld(transaction, data)
                if held:
                    print(operation)
                    print("Transaction has lock, added to final schedule\n")
                    final_schedule.append(operation)
                else:
                    # TODO: Handle deadlock
                    print(operation)
                    print("Transaction acquires lock. ", end="")
                    # Acquire lock
                    lockManager.acquireLock(transaction, data)
                    # Check if transaction holds lock of data or still in queue
                    status = lockManager.isHeld(transaction, data)
                    if status:
                        print("Transaction holds the lock, added to final schedule\n")
                        final_schedule.append(operation)
                    else:
                        print("Transaction goes to queue and waits for lock\n")
                        lockManager.setTransactionWaiting(transaction, True)
                        waiting_queue.append(operation)
            else:
                print(operation)
                print("Transaction releases locks\n")
                lockManager.releaseAllLocks(transaction)
                final_schedule.append(operation)
        else:
            waiting_queue.append(operation)
            print(operation)
            print("Transaction waiting, goes to queue\n")
            # print(waiting_queue)

        # print("Remove " + str(schedule[i]) + " from schedule\n")
        schedule.pop(i)
        # print(len(schedule))

print(final_schedule)