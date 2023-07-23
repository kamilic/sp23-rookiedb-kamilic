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

        boolean canBeParentLock = parentContext == null || LockType.canBeParentLock(
                parentContext.getExplicitLockType(transaction),
                requestType
        );

        // same lock type should return immediately
        if (requestType.equals(explicitLockType)) {
            return;
        }

        // The current lock type can effectively substitute the requested type
        if (requestType.equals(LockType.NL)) {
            lockContext.release(transaction);
        } else if (explicitLockType.equals(LockType.IS) && requestType.equals(LockType.S)) {
            lockContext.escalate(transaction);
        } else if (explicitLockType.equals(LockType.IX) && requestType.equals(LockType.S)) {
            lockContext.promote(transaction, LockType.SIX);
        } else {
            if (!canBeParentLock) {
                LockUtil.ensureParentContext(lockContext.parent, requestType);
            }

            if (LockType.compatible(requestType, explicitLockType)) {
                lockContext.acquire(transaction, requestType);
            } else if (LockType.substitutable(requestType, explicitLockType)) {
                lockContext.promote(transaction, requestType);
            } else {
                lockContext.acquire(transaction, requestType);
            }
        }
    }

    /**
     * @param releaseAction `true` will give a bottom-up order
     *                      `false` will give a top-down order for lock acquire/promote
     */
    static public List<LockContext> getOrderedContext(List<LockContext> contexts, boolean releaseAction) {
        contexts.sort(new Comparator<LockContext>() {
            @Override
            public int compare(LockContext o1, LockContext o2) {
                int size1 = o1.name.getNames().size();
                int size2 = o2.name.getNames().size();
                return releaseAction ? size2 - size1 : size1 - size2;
            }
        });

        return contexts;
    }

    static public void ensureParentContext(LockContext parentContext, LockType childLock) {
        TransactionContext transaction = TransactionContext.getTransaction();
        List<LockContext> allParentContexts = new ArrayList<>();
        LockType parentLock = LockType.parentLock(childLock);

        LockContext currentLockContext = parentContext;
        while (currentLockContext != null) {
            allParentContexts.add(currentLockContext);
            currentLockContext = currentLockContext.parent;
        }

        List<LockContext> sortedParentContexts = LockUtil.getOrderedContext(allParentContexts, false);

        for (LockContext ctx: sortedParentContexts) {
            LockType type = ctx.getExplicitLockType(transaction);

            if (type.equals(parentLock)) {
                continue;
            }

            if (type.equals(LockType.NL)) {
                ctx.acquire(transaction, parentLock);
            } else if (LockType.substitutable(parentLock, type)) {
                ctx.promote(transaction, parentLock);
            }
        }
    }

    // TODO(proj4_part2) add any helper methods you want
}
