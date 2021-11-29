import sys

class Lock:
    def __init__(self, data):
        # Data item
        self.data = data
        # Queue of transactions acquiring lock, head of queue holds the lock
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
        # Name of transaction
        self.transaction = transaction
        # Waiting status, true if transaction is waiting for a lock (in waiting queue)
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

    # Transaction attempts to acquire lock of data
    def acquireLock(self, transaction, data):
        index = self.findLock(data)
        if index != -1:
            # Wait-die mechanism
            held = self.locks[index].heldBy()
            if held:
                index_acquire = self.findTransaction(transaction)
                index_held = self.findTransaction(held)
                if index_acquire < index_held:
                    self.locks[index].enqueue(transaction)
                    return True
                else:
                    return False
            else:
                self.locks[index].enqueue(transaction)
                return True    
        else:
            lock = Lock(data)
            lock.enqueue(transaction)
            self.locks.append(lock)
            return True

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

    # Remove transaction from lock queue
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


# Read schedule from file
filename = sys.argv[1]
directory = '../test/' + filename
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

print('Schedule:')
print(schedule)
print('\n')

# Execute schedule
i = 0
while len(schedule) > 0 or len(waiting_queue) > 0:
    
    # Execute waiting queue if possible
    if len(waiting_queue) != 0:
        print('-- EXECUTING WAITING QUEUE --')
        print('Waiting Queue:')
        print(waiting_queue)
        print('\n')
        
        i_queue = 0
        while i_queue < len(waiting_queue):
            operation_queue = waiting_queue[i_queue]
            transaction_queue = operation_queue[0]
            action_queue = operation_queue[1]
            data_queue = operation_queue[2]

            # Check for locks if action is not commit
            if action_queue != 'COMMIT':
                print('Executing: ', end="")
                print(operation_queue)
                held_queue = lockManager.isHeld(transaction_queue, data_queue)
                # Add operation to final schedule if lock is held
                if held_queue:
                    print("Transaction in queue has lock, added to final schedule\n")
                    lockManager.setTransactionWaiting(transaction_queue, False)
                    final_schedule.append(operation_queue)
                    waiting_queue.pop(i_queue)
                    i_queue -= 1
                # Lock is not held
                else:
                    index_lock = lockManager.findLock(data_queue)
                    if index_lock != -1:
                        waited = lockManager.locks[index_lock].isWaitedByTransaction(transaction_queue)
                        if not waited:
                            lock_queue = lockManager.acquireLock(transaction_queue, data_queue)
                            if lock_queue:
                                print("Lock available, added to lock queue\n")
                            else:
                                print("Lock still not available, keep waiting\n")
                        else:
                            print("Still waiting, skip queue\n")
            else:
                # Release all locks when commiting
                if not lockManager.isTransactionWaiting(transaction_queue):
                    print('Executing: ', end="")
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
            if action != 'COMMIT':
                print('Executing: ', end="")
                print(operation)
                held = lockManager.isHeld(transaction, data)
                # Add operation to final schedule if lock is held
                if held:
                    print("Transaction has lock, added to final schedule\n")
                    final_schedule.append(operation)
                    schedule.pop(i)
                # Lock is not held
                else:
                    print("Transaction attempts to acquire lock. ", end="")
                    # Acquire lock
                    acquire = lockManager.acquireLock(transaction, data)
                    if acquire:
                        # Check if transaction holds lock of data or still in queue
                        status = lockManager.isHeld(transaction, data)
                        if status:
                            print("Transaction holds the lock, added to final schedule\n")
                            final_schedule.append(operation)
                        else:
                            print("Transaction goes to queue and waits for lock\n")
                            lockManager.setTransactionWaiting(transaction, True)
                            waiting_queue.append(operation)
                        schedule.pop(i)
                    # Rollback when fails to acquire lock (newer transaction, due to wait-die scheme)
                    else:
                        print("New transaction can't wait, rollback")
                        print("Rollback " + str(transaction) + "\n")

                        # Delete operations of newer transaction from waiting queue
                        new_waiting = []
                        i_waiting = 0
                        while i_waiting < len(waiting_queue):
                            if waiting_queue[i_waiting][0] == transaction:
                                deleted_operation = waiting_queue.pop(i_waiting)
                                lockManager.removeTransactionFromData(deleted_operation[0], deleted_operation[2])
                                new_waiting.append(deleted_operation)
                                i_waiting -= 1
                            i_waiting += 1

                        # Delete operations of newer transaction from final schedule
                        new_final = []
                        i_final = 0
                        while i_final < len(final_schedule):
                            if final_schedule[i_final][0] == transaction:
                                new_final.append(final_schedule[i_final])
                                final_schedule.pop(i_final)
                                i_final -= 1
                            i_final += 1

                        # Delete remaining operations of newer transaction from schedule
                        new_schedule = []
                        i_schedule = i
                        while i_schedule < len(schedule):
                            if schedule[i_schedule][0] == transaction:
                                new_schedule.append(schedule[i_schedule])
                                schedule.pop(i_schedule)
                                i_schedule -= 1
                            i_schedule += 1

                        # Add transaction to end of schedule
                        schedule.extend(new_final)
                        schedule.extend(new_waiting)
                        schedule.extend(new_schedule)
                        print('New schedule after rollback:')
                        print(schedule)
                        print('\n')

                        # Release all locks from newer transaction
                        lockManager.releaseAllLocks(transaction)
            # Transaction commits                            
            else:
                print('Executing: ', end="")
                print(operation)
                print("Transaction releases locks\n")
                lockManager.releaseAllLocks(transaction)
                final_schedule.append(operation)
                schedule.pop(i)
        # Transaction is in waiting queue
        else:
            waiting_queue.append(operation)
            print('Executing: ', end="")
            print(operation)
            print("Transaction waiting, goes to queue\n")
            schedule.pop(i)

print('Final Schedule:')
print(final_schedule)