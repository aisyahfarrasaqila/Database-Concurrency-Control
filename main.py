from os import truncate, wait
import sys

class Lock:
    def __init__(self, data):
        self.data = data
        self.transactions = []

    def enqueue(self, transaction):
        self.transactions.append(transaction)

    def dequeue(self):
        self.transactions.pop(0)

    def heldBy(self):
        if len(self.transactions) != 0:
            return self.transactions[0]
        return None

    def isHeldByTransaction(self, transaction):
        if len(self.transactions) > 0:
            return True if self.transactions[0] == transaction else False
        return False

    def isWaitedByTransaction(self, transaction):
        found = False
        i = 1
        while not found and i < len(self.transactions):
            if self.transactions[i] == transaction:
                found = True
            i += 1
        return found

    def removeTransaction(self, transaction):
        if transaction in self.transactions:
            self.transactions.remove(transaction)

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
            if len(lock.transactions) != 0:
                if lock.transactions[0] == transaction:
                    locks.append(lock)
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

    def removeTransactionFromData(self, transaction, data):
        index = self.findLock(data)
        if index != -1:
            self.locks[index].removeTransaction(transaction)

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
        # Deadlock happens when transaction is trying to acquire lock of data and that data is held
        # by another transaction that is on the queue for a lock currently held by transaction.
        # This function returns the other transaction
        index = self.findLock(data)
        if index != -1:
            # Transaction currently holds lock of data
            heldBy = self.locks[index].heldBy()

            if heldBy:
                locks = self.transactionLocks(transaction)
                for lock in locks:
                    if lock.isWaitedByTransaction(heldBy):
                        return heldBy
        return None


# # Test for isDeadlock
# lockManager = LockManager()
# lockManager.addTransaction(('T2'))
# lockManager.addTransaction('T1')
# lockManager.acquireLock('T1', 'B')
# lockManager.acquireLock('T2', 'A')
# lockManager.acquireLock('T2', 'B')
# deadlock = lockManager.isDeadlock('T1', 'A')
# print(deadlock)

# Read schedule from file
directory = sys.argv[1]
file = open(directory, 'r')

# Make schedule
lockManager = LockManager()

schedule = []
final_schedule = []
waiting_queue = []
deadlock_queue = []

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
        print(waiting_queue)
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
                    lockManager.setTransactionWaiting(transaction_queue, False)
                    final_schedule.append(operation_queue)
                    waiting_queue.pop(i_queue)
                    i_queue -= 1
                # Skip queue if lock is not held
                else:
                    print(operation_queue)
                    print("Still waiting, skip queue\n")
            else:
                # Release all locks when commiting
                if not lockManager.isTransactionWaiting(transaction_queue):
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
                    print(operation)
                    # Check for deadlock when trying to acquire a lock
                    deadlock = lockManager.isDeadlock(transaction, data)
                    if not deadlock:
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
                        print('Deadlock')
                        # Deadlock Recovery: Older lock request is selected as victim
                        index_acquire = lockManager.findTransaction(transaction)
                        index_deadlock = lockManager.findTransaction(deadlock)

                        print("Rollback " + str(deadlock))
                        # Delete operations of old request's transaction from waiting queue
                        new_waiting = []
                        i_waiting = 0
                        while i_waiting < len(waiting_queue):
                            if waiting_queue[i_waiting][0] == deadlock:
                                deleted_operation = waiting_queue.pop(i_waiting)
                                lockManager.removeTransactionFromData(deleted_operation[0], deleted_operation[2])
                                new_waiting.append(deleted_operation)
                                i_waiting -= 1
                            i_waiting += 1

                        # Delete operations of old request's transaction from final schedule and add to waiting queue
                        i_final = 0
                        while i_final < len(final_schedule):
                            if final_schedule[i_final][0] == deadlock:
                                waiting_queue.append(final_schedule[i_final])
                                final_schedule.pop(i_final)
                                i_final -= 1
                            i_final += 1

                        # Add new_waiting to waiting queue and acquire lock
                        waiting_queue.extend(new_waiting)

                        # Release all locks from old request
                        lockManager.setTransactionWaiting(deadlock, True)
                        lockManager.releaseAllLocks(deadlock)

                        # Acquire lock for new request
                        print("Transaction holds the lock, added to final schedule\n")
                        lockManager.acquireLock(transaction, data)
                        final_schedule.append(operation)
                        
                        # Acquire lock for old request (add to lock queue)
                        for waiting in waiting_queue:
                            if waiting[0] == deadlock:
                                lockManager.acquireLock(waiting[0], waiting[2])
                            
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