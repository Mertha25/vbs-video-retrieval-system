import React from 'react'

export const Layout: React.FC<{ filter: React.ReactNode, children: React.ReactNode }> = ({ filter, children }) => (
  <div className="
    flex flex-col gap-6
    md:flex-row
    md:gap-10
    w-full
    max-w-full
    min-h-[calc(100vh-120px)]
    ">
    {/* Sidebar (filter) */}
    <aside
      className="
        w-full
        md:w-[28vw]
        md:min-w-[220px]
        md:max-w-[380px]
        md:sticky
        md:top-[90px]
        p-4
        bg-white
        rounded-xl
        shadow-md
        mb-4
        md:mb-0
        flex-shrink-0
        transition-all
        duration-200
      "
    >
      {filter}
    </aside>
    {/* Main content (videos) */}
    <main className="flex-1 min-w-0 px-2 md:px-6 pt-4 pb-8 bg-transparent">
      {children}
    </main>
  </div>
)

