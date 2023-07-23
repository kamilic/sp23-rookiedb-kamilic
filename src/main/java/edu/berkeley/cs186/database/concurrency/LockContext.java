package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.databox.Type;

import javax.naming.Context;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * LockContext wraps around LockManager to provide the hierarchical structure
 * of multigranularity locking. Calls to acquire/release/etc. locks should
 * be mostly done through a LockContext, which provides access to locking
 * methods at a certain point in the hierarchy (database, table X, etc.)
 */
public class LockContext {
    // You should not remove any of these fields. You may add additional
    // fields/methods as you see fit.

    // The underlying lock manager.
    protected final LockManager lockman;

    // The parent LockContext object, or null if this LockContext is at the top of the hierarchy.
    protected final LockContext parent;

    // The name of the resource this LockContext represents.
    protected ResourceName name;

    // Whether this LockContext is readonly. If a LockContext is readonly, acquire/release/promote/escalate should
    // throw an UnsupportedOperationException.
    protected boolean readonly;

    // A mapping between transaction numbers, and the number of locks on children of this LockContext
    // that the transaction holds.
    protected final Map<Long, Integer> numChildLocks;

    // You should not modify or use this directly.
    protected final Map<String, LockContext> children;

    // Whether or not any new child LockContexts should be marked readonly.
    protected boolean childLocksDisabled;

    public LockContext(LockManager lockman, LockContext parent, String name) {
        this(lockman, parent, name, false);
    }

    protected LockContext(LockManager lockman, LockContext parent, String name,
                          boolean readonly) {
        this.lockman = lockman;
        this.parent = parent;
        if (parent == null) {
            this.name = new ResourceName(name);
        } else {
            this.name = new ResourceName(parent.getResourceName(), name);
        }
        this.readonly = readonly;
        this.numChildLocks = new ConcurrentHashMap<>();
        this.children = new ConcurrentHashMap<>();
        this.childLocksDisabled = readonly;
    }

    /**
     * Gets a lock context corresponding to `name` from a lock manager.
     */
    public static LockContext fromResourceName(LockManager lockman, ResourceName name) {
        Iterator<String> names = name.getNames().iterator();
        LockContext ctx;
        String n1 = names.next();
        ctx = lockman.context(n1);
        while (names.hasNext()) {
            String n = names.next();
            ctx = ctx.childContext(n);
        }
        return ctx;
    }

    /**
     * Get the name of the resource that this lock context pertains to.
     */
    public ResourceName getResourceName() {
        return name;
    }

    /**
     * Acquire a `lockType` lock, for transaction `transaction`.
     * <p>
     * Note: you must make any necessary updates to numChildLocks, or else calls
     * to LockContext#getNumChildren will not work properly.
     *
     * @throws InvalidLockException          if the request is invalid
     * @throws DuplicateLockRequestException if a lock is already held by the
     *                                       transaction.
     * @throws UnsupportedOperationException if context is readonly
     */
    public void acquire(TransactionContext transaction, LockType lockType)
            throws InvalidLockException, DuplicateLockRequestException {
        checkReadOnlyOrThrow();
        // TODO(proj4_part2): implement
        ResourceName resourceName = getResourceName();
        LockType parentLockType = parent != null ? parent.getExplicitLockType(transaction) : null;
        boolean isParentLockInvalid = parentLockType != null && !LockType.canBeParentLock(parentLockType, lockType);

        if (isParentLockInvalid) {
            throw new InvalidLockException("Attempting to acquire an " +
                    lockType + " lock with an " + parentLockType + " parent lock");
        }

        boolean isLockTypeIsISOrS = lockType.equals(LockType.S) || lockType.equals(LockType.IS);
        if (hasSIXAncestor(transaction) && isLockTypeIsISOrS) {
            throw new InvalidLockException("Lock is invalid.");
        }

        lockman.acquire(transaction, resourceName, lockType);
        if (parent != null) {
            parent.numChildLocks.put(transaction.getTransNum(), parent.getNumChildren(transaction) + 1);
        }
    }

    /**
     * Release `transaction`'s lock on `name`.
     * <p>
     * Note: you *must* make any necessary updates to numChildLocks, or
     * else calls to LockContext#getNumChildren will not work properly.
     *
     * @throws NoLockHeldException           if no lock on `name` is held by `transaction`
     * @throws InvalidLockException          if the lock cannot be released because
     *                                       doing so would violate multigranularity locking constraints
     * @throws UnsupportedOperationException if context is readonly
     */
    public void release(TransactionContext transaction)
            throws NoLockHeldException, InvalidLockException {
        // TODO(proj4_part2): implement
        checkReadOnlyOrThrow();
        ResourceName resourceName = getResourceName();
        List<Lock> allChildLocks = descendantLocks(transaction);
        LockType currentLockType = lockman.getLockType(transaction, resourceName);

        for (Lock l : allChildLocks) {
            if (LockType.canBeParentLock(currentLockType, l.lockType)) {
                throw new InvalidLockException("Attempting to release an " +
                        currentLockType + "lock containing an " + l.lockType + " lock on its children");
            }
        }


        lockman.release(transaction, resourceName);
        if (parent != null) {
            parent.numChildLocks.put(transaction.getTransNum(), parent.getNumChildren(transaction) - 1);
        }
    }

    private void checkReadOnlyOrThrow() throws UnsupportedOperationException {
        if (this.readonly) {
            throw new UnsupportedOperationException("Current resource is readonly");
        }
    }


    /**
     * Promote `transaction`'s lock to `newLockType`. For promotion to SIX from
     * IS/IX, all S and IS locks on descendants must be simultaneously
     * released. The helper function sisDescendants may be helpful here.
     * <p>
     * Note: you *must* make any necessary updates to numChildLocks, or else
     * calls to LockContext#getNumChildren will not work properly.
     *
     * @throws DuplicateLockRequestException if `transaction` already has a
     *                                       `newLockType` lock
     * @throws NoLockHeldException           if `transaction` has no lock
     * @throws InvalidLockException          if the requested lock type is not a
     *                                       promotion or promoting would cause the lock manager to enter an invalid
     *                                       state (e.g. IS(parent), X(child)). A promotion from lock type A to lock
     *                                       type B is valid if B is substitutable for A and B is not equal to A, or
     *                                       if B is SIX and A is IS/IX/S, and invalid otherwise. hasSIXAncestor may
     *                                       be helpful here.
     * @throws UnsupportedOperationException if context is readonly
     */
    public void promote(TransactionContext transaction, LockType newLockType)
            throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        // TODO(proj4_part2): implement
        checkReadOnlyOrThrow();
        ResourceName resourceName = getResourceName();
        LockType currentLockType = lockman.getLockType(transaction, resourceName);
        boolean isSIXAncestorNotAllowLock = currentLockType.equals(LockType.IS) ||
                currentLockType.equals(LockType.IX) ||
                currentLockType.equals(LockType.S);

        if (isSIXAncestorNotAllowLock && hasSIXAncestor(transaction)) {
            throw new InvalidLockException("Not allow to promote a " + currentLockType +
                    " lock which ancestor is SIX lock to " + newLockType
            );
        }

        lockman.promote(transaction, resourceName, newLockType);
        if (newLockType.equals(LockType.SIX)) {
            List<ResourceName> sisNames = sisDescendants(transaction);
            for (ResourceName sisName : sisNames) {
                LockContext descendantCtx = LockContext.fromResourceName(lockman, sisName);
                descendantCtx.release(transaction);
            }
        }
    }

    /**
     * Escalate `transaction`'s lock from descendants of this context to this
     * level, using either an S or X lock. There should be no descendant locks
     * after this call, and every operation valid on descendants of this context
     * before this call must still be valid. You should only make *one* mutating
     * call to the lock manager, and should only request information about
     * TRANSACTION from the lock manager.
     * <p>
     * For example, if a transaction has the following locks:
     * <p>
     * IX(database)
     * /         \
     * IX(table1)    S(table2)
     * /      \
     * S(table1 page3)  X(table1 page5)
     * <p>
     * then after table1Context.escalate(transaction) is called, we should have:
     * <p>
     * IX(database)
     * /         \
     * X(table1)     S(table2)
     * <p>
     * You should not make any mutating calls if the locks held by the
     * transaction do not change (such as when you call escalate multiple times
     * in a row).
     * <p>
     * Note: you *must* make any necessary updates to numChildLocks of all
     * relevant contexts, or else calls to LockContext#getNumChildren will not
     * work properly.
     *
     * @throws NoLockHeldException           if `transaction` has no lock at this level
     * @throws UnsupportedOperationException if context is readonly
     */
    public void escalate(TransactionContext transaction) throws NoLockHeldException {
        // TODO(proj4_part2): implement
        checkReadOnlyOrThrow();
        LockType currentLockType = lockman.getLockType(transaction, getResourceName());

        if (currentLockType.equals(LockType.NL)) {
            throw new NoLockHeldException("No lock held at this transaction");
        }

        // find out all descendant of this context
        List<Lock> descendantLocks = descendantLocks(transaction);
        // calculate out the escalate lock type
        LockType targetLockType = descendantLocks
                .stream()
                .map(l -> l.lockType)
                .reduce(LockType.escalatedLock(currentLockType), (res, l) -> {
                    LockType escalatedLock = LockType.escalatedLock(l);
                    if (res.equals(LockType.X) || escalatedLock.equals(LockType.X)) {
                        return LockType.X;
                    }

                    if (res.equals(LockType.S) || escalatedLock.equals(LockType.S)) {
                        return LockType.S;
                    }

                    return res;
                });

        if (!currentLockType.equals(targetLockType)) {
            List<ResourceName> resourceNames = descendantLocks.stream()
                    .map(l -> l.name).collect(Collectors.toList());

            resourceNames.add(getResourceName());

            // Why using `acquireAndRelease`?
            // The hints on the comment: You should only make *one* mutating call to the lock manager.
            lockman.acquireAndRelease(transaction, getResourceName(), targetLockType, resourceNames);

            for (Lock l : descendantLocks) {
                LockContext lockContext = LockContext.fromResourceName(lockman, l.name);
                LockContext parent = lockContext.parent;
                if (parent != null) {
                    parent.numChildLocks.put(transaction.getTransNum(), parent.getNumChildren(transaction) - 1);
                }
            }
        }


    }

    /**
     * Get the type of lock that `transaction` holds at this level, or NL if no
     * lock is held at this level.
     */
    public LockType getExplicitLockType(TransactionContext transaction) {
        if (transaction == null) return LockType.NL;
        // TODO(proj4_part2): implement
        return lockman.getLockType(transaction, getResourceName());
    }

    /**
     * Gets the type of lock that the transaction has at this level, either
     * implicitly (e.g. explicit S lock at higher level implies S lock at this
     * level) or explicitly. Returns NL if there is no explicit nor implicit
     * lock.
     */
    public LockType getEffectiveLockType(TransactionContext transaction) {
        if (transaction == null) return LockType.NL;
        LockContext currentContext = this;
        LockType result = LockType.NL;
        do {
            LockType type = currentContext.getExplicitLockType(transaction);
            if (!type.equals(LockType.NL) && !type.isIntent()) {
                result = type;
                break;
            }
            currentContext = currentContext.parent;
        }
        while (currentContext != null);

        // TODO(proj4_part2): implement
        return result;
    }

    /**
     * Helper method to see if the transaction holds a SIX lock at an ancestor
     * of this context
     *
     * @param transaction the transaction
     * @return true if holds a SIX at an ancestor, false if not
     */
    private boolean hasSIXAncestor(TransactionContext transaction) {
        // TODO(proj4_part2): implement
        LockContext currentContext = this;
        do {
            LockType type = currentContext.getExplicitLockType(transaction);
            if (type.equals(LockType.SIX)) {
                return true;
            }
            currentContext = currentContext.parent;
        } while (currentContext != null);
        return false;
    }

    /**
     * Helper method to get a list of all locks that
     * are descendants of current context for the given transaction.
     *
     * @param transaction the given transaction
     * @return a list of ResourceNames of descendants
     */
    private List<Lock> descendantLocks(TransactionContext transaction) {
        ResourceName resourceName = getResourceName();
        List<Lock> allTransLocks = lockman.getLocks(transaction);
        List<Lock> allChildLocks = new ArrayList<>();
        for (Lock l : allTransLocks) {
            if (l.name.isDescendantOf(resourceName)) {
                allChildLocks.add(l);
            }
        }
        return allChildLocks;
    }

    /**
     * Helper method to get a list of resourceNames of all locks that are S or
     * IS and are descendants of current context for the given transaction.
     *
     * @param transaction the given transaction
     * @return a list of ResourceNames of descendants which the transaction
     * holds an S or IS lock.
     */
    private List<ResourceName> sisDescendants(TransactionContext transaction) {
        // TODO(proj4_part2): implement
        List<Lock> descendants = descendantLocks(transaction);
        return descendants
                .stream()
                .filter(l -> l.lockType.equals(LockType.S) || l.lockType.equals(LockType.IS))
                .map(l -> l.name)
                .collect(Collectors.toList());
    }

    /**
     * Disables locking descendants. This causes all new child contexts of this
     * context to be readonly. This is used for indices and temporary tables
     * (where we disallow finer-grain locks), the former due to complexity
     * locking B+ trees, and the latter due to the fact that temporary tables
     * are only accessible to one transaction, so finer-grain locks make no
     * sense.
     */
    public void disableChildLocks() {
        this.childLocksDisabled = true;
    }

    /**
     * Gets the parent context.
     */
    public LockContext parentContext() {
        return parent;
    }

    /**
     * Gets the context for the child with name `name` and readable name
     * `readable`
     */
    public synchronized LockContext childContext(String name) {
        LockContext temp = new LockContext(lockman, this, name,
                this.childLocksDisabled || this.readonly);
        LockContext child = this.children.putIfAbsent(name, temp);
        if (child == null) child = temp;
        return child;
    }

    /**
     * Gets the context for the child with name `name`.
     */
    public synchronized LockContext childContext(long name) {
        return childContext(Long.toString(name));
    }

    /**
     * Gets the number of locks held on children a single transaction.
     */
    public int getNumChildren(TransactionContext transaction) {
        return numChildLocks.getOrDefault(transaction.getTransNum(), 0);
    }

    @Override
    public String toString() {
        return "LockContext(" + name.toString() + ")";
    }
}

