package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

import java.util.*;

/**
 * LockUtil is a declarative layer which simplifies multigranularity lock
 * acquisition for the user (you, in the last task of Part 2). Generally
 * speaking, you should use LockUtil for lock acquisition instead of calling
 * LockContext methods directly.
 */
public class LockUtil {
    /**
     * Ensure that the current transaction can perform actions requiring
     * `requestType` on `lockContext`.
     * <p>
     * `requestType` is guaranteed to be one of: S, X, NL.
     * <p>
     * This method should promote/escalate/acquire as needed, but should only
     * grant the least permissive set of locks needed. We recommend that you
     * think about what to do in each of the following cases:
     * - The current lock type can effectively substitute the requested type
     * - The current lock type is IX and the requested lock is S
     * - The current lock type is an intent lock
     * - None of the above: In this case, consider what values the explicit
     * lock type can be, and think about how ancestor looks will need to be
     * acquired or changed.
     * <p>
     * You may find it useful to create a helper method that ensures you have
     * the appropriate locks on all ancestors.
     */
    public static void ensureSufficientLockHeld(LockContext lockContext, LockType requestType) {
        // requestType must be S, X, or NL
        assert (requestType == LockType.S || requestType == LockType.X || requestType == LockType.NL);

        // Do nothing if the transaction or lockContext is null
        TransactionContext transaction = TransactionContext.getTransaction();
        if (transaction == null || lockContext == null) return;

        // You may find these variables useful
        LockContext parentContext = lockContext.parentContext();
        LockType effectiveLockType = lockContext.getEffectiveLockType(transaction);
        LockType explicitLockType = lockContext.getExplicitLockType(transaction);

        // same lock type should return immediately
        if (requestType.equals(explicitLockType)) {
            return;
        }

        // The current lock type can effectively substitute the requested type
        if (requestType.equals(LockType.NL)) {
            lockContext.release(transaction);
            return;
        }

        if (explicitLockType.equals(LockType.NL) && effectiveLockType.equals(LockType.NL)) {
            LockUtil.ensureParentContext(lockContext.parent, requestType);
            lockContext.acquire(transaction, requestType);
        }

        if (explicitLockType.equals(LockType.IS) && requestType.equals(LockType.S)) {
            lockContext.escalate(transaction);
        }
    }

    static public void ensureParentContext(LockContext parentContext, LockType childLock) {
        LockContext currentLockContext = parentContext;
        Stack<LockContext> allParentContexts = new Stack<>();
        LockType parentLock = LockType.parentLock(childLock);

        while (currentLockContext != null) {
            allParentContexts.push(currentLockContext);
            currentLockContext = currentLockContext.parent;
        }

        while (!allParentContexts.empty()) {
            currentLockContext = allParentContexts.pop();
            currentLockContext.acquire(TransactionContext.getTransaction(), parentLock);
        }
    }

    // TODO(proj4_part2) add any helper methods you want
}
